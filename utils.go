package ebi

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/thalesfsp/customerror"
)

//////
// Update metrics process.
//////

// calculateSystemPressure calculates the system pressure based on the provided
// node stats. This implementation creates three main metrics:
//
// Overall Pressure (OverallPressure):
//
// - Scale: 0-100%
// - Composition: 60% memory pressure + 40% write pressure
//
// Memory Pressure (MemoryPressure):
//
// - Scale: 0-100%
// - Composition: 40% JVM heap usage, 30% circuit breaker status, 30% memory
// pressure during indexing.
//
// Write Pressure (WritePressure):
//
// Scale: 0-100%
// Based on: 50% write load, 30% merge pressure, 20% throttling status.
func calculateSystemPressure(nodeStats NodesStatsResponse) SystemPressure {
	var pressure SystemPressure

	// Calcula pressão de memória baseado em:
	// - JVM heap usage
	// - Circuit breaker status
	// - Indexing pressure memory
	var memoryPressurePoints float64

	var totalMemoryFactors float64

	for _, node := range nodeStats.Nodes {
		if node.JVM != nil && node.JVM.Mem != nil {
			// JVM Heap contribui 40% da pressão de memória
			memoryPressurePoints += float64(node.JVM.Mem.HeapUsedPercent) * 0.4
			totalMemoryFactors += 40
		}

		if node.Breakers != nil && node.Breakers.Parent != nil {
			// Circuit breaker contribui 30% da pressão de memória
			breakerPressure := float64(node.Breakers.Parent.EstimatedSizeInBytes) /
				float64(node.Breakers.Parent.LimitSizeInBytes) * 100
			memoryPressurePoints += breakerPressure * 0.3
			totalMemoryFactors += 30
		}

		// Indexing pressure memory contribui 30% da pressão de memória
		if pressure := node.IndexingPressure.Memory; pressure != nil {
			if pressure.LimitInBytes > 0 {
				currentPressure := float64(pressure.Current.AllInBytes) /
					float64(pressure.LimitInBytes) * 100
				memoryPressurePoints += currentPressure * 0.3
				totalMemoryFactors += 30
			}
		}
	}

	// Normaliza a pressão de memória
	if totalMemoryFactors > 0 {
		pressure.MemoryPressure = memoryPressurePoints / totalMemoryFactors * 100
	}

	// Calcula pressão de escrita baseado em:
	// - Write load
	// - Throttle status
	// - Merge pressure
	var writePressurePoints float64

	var totalWriteFactors float64

	for _, node := range nodeStats.Nodes {
		if node.Indices != nil {
			// Write load contribui 50% da pressão de escrita
			if writeLoad := node.Indices.Indexing.WriteLoad; writeLoad > 0 {
				// Normaliza o write load (assume que >1 é 100% pressão)
				writePressure := math.Min(writeLoad*100, 100)
				writePressurePoints += writePressure * 0.5
				totalWriteFactors += 50
			}

			// Merge status contribui 30% da pressão de escrita
			if merges := node.Indices.Merges; merges != nil && merges.TotalTimeInMillis > 0 {
				// Se está gastando mais de 10% do tempo em merges, considera pressão
				mergePressure := math.Min(
					float64(merges.TotalTimeInMillis)/
						float64(node.JVM.UptimeInMillis)*1000, 100)
				writePressurePoints += mergePressure * 0.3
				totalWriteFactors += 30
			}

			// Throttle status contribui 20% da pressão de escrita
			if node.Indices.Indexing.IsThrottled {
				writePressurePoints += 100 * 0.2
				totalWriteFactors += 20
			}
		}
	}

	// Normaliza a pressão de escrita
	if totalWriteFactors > 0 {
		pressure.WritePressure = writePressurePoints / totalWriteFactors * 100
	}

	// Pressão geral é uma composição de memória (60%) e escrita (40%)
	pressure.OverallPressure = (pressure.MemoryPressure * 0.6) +
		(pressure.WritePressure * 0.4)

	return pressure
}

// updateIndexMetrics fetches index and node stats from Elasticsearch and
// updates the provided IndexMetrics struct.
//
//nolint:nestif
func updateIndexMetrics(
	ctx context.Context,
	indexName string,
	client *elasticsearch.Client,
	metrics *IndexMetrics,
) error {
	//////
	// Indices stats.
	//////

	indexStats, err := client.Indices.Stats(
		client.Indices.Stats.WithContext(ctx),
		client.Indices.Stats.WithIndex(indexName),
		client.Indices.Stats.WithMetric("segments", "indexing", "translog"),
	)
	if err != nil {
		return customerror.NewFailedToError("get index stats", customerror.WithError(err))
	}

	defer indexStats.Body.Close()

	// Parse index stats into our structured type
	var indexStatsResp IndexStatsResponse
	if err := json.NewDecoder(indexStats.Body).Decode(&indexStatsResp); err != nil {
		return customerror.NewFailedToError("parse index stats", customerror.WithError(err))
	}

	// Get specific index stats
	indexData, exists := indexStatsResp.Indices[indexName]
	if !exists {
		return customerror.NewFailedToError("find index stats", customerror.WithError(
			fmt.Errorf("index %s not found in response", indexName)))
	}

	// Update metrics from the structured data
	if indexData.Total != nil && indexData.Total.Segments != nil {
		metrics.SegmentsCount = indexData.Total.Segments.Count
		metrics.SegmentsMemory = indexData.Total.Segments.MemoryInBytes
	}

	if indexData.Total != nil && indexData.Total.Translog != nil {
		metrics.TranslogSize = indexData.Total.Translog.SizeInBytes
	}

	//////
	// Nodes stats.
	//////

	// Get nodes stats for JVM and circuit breakers.
	nodesStats, err := client.Nodes.Stats(
		client.Nodes.Stats.WithContext(ctx),
		client.Nodes.Stats.WithMetric("jvm", "breaker", "indexing_pressure", "indices"),
	)
	if err != nil {
		return customerror.NewFailedToError("get nodes stats", customerror.WithError(err))
	}

	defer nodesStats.Body.Close()

	// Parse nodes stats.
	var nodesStatsResp NodesStatsResponse
	if err := json.NewDecoder(nodesStats.Body).Decode(&nodesStatsResp); err != nil {
		return customerror.NewFailedToError("parse nodes stats", customerror.WithError(err))
	}

	// Calculate JVM heap usage and circuit breaker ratios
	var totalHeap float64

	var usedHeap float64

	for _, nodeData := range nodesStatsResp.Nodes {
		// Get JVM info
		if nodeData.JVM != nil && nodeData.JVM.Mem != nil {
			totalHeap += float64(nodeData.JVM.Mem.HeapMaxInBytes)
			usedHeap += float64(nodeData.JVM.Mem.HeapUsedInBytes)
		}

		// Get circuit breaker info
		if nodeData.Breakers != nil {
			if nodeData.Breakers.Parent != nil {
				metrics.CircuitBreakers["parent"] = float64(nodeData.Breakers.Parent.EstimatedSizeInBytes) /
					float64(nodeData.Breakers.Parent.LimitSizeInBytes)
			}

			if nodeData.Breakers.FieldData != nil {
				metrics.CircuitBreakers["fielddata"] = float64(nodeData.Breakers.FieldData.EstimatedSizeInBytes) /
					float64(nodeData.Breakers.FieldData.LimitSizeInBytes)
			}

			if nodeData.Breakers.InFlightRequests != nil {
				metrics.CircuitBreakers["in_flight_requests"] = float64(nodeData.Breakers.InFlightRequests.EstimatedSizeInBytes) /
					float64(nodeData.Breakers.InFlightRequests.LimitSizeInBytes)
			}

			if nodeData.Breakers.ModelInference != nil {
				metrics.CircuitBreakers["model_inference"] = float64(nodeData.Breakers.ModelInference.EstimatedSizeInBytes) /
					float64(nodeData.Breakers.ModelInference.LimitSizeInBytes)
			}

			if nodeData.Breakers.Request != nil {
				metrics.CircuitBreakers["request"] = float64(nodeData.Breakers.Request.EstimatedSizeInBytes) /
					float64(nodeData.Breakers.Request.LimitSizeInBytes)
			}
		}
	}

	metrics.JVMHeapUsage = usedHeap / totalHeap

	// Calculate and update system pressure metrics.
	metrics.SystemPressure = calculateSystemPressure(nodesStatsResp)

	return nil
}

//////
// Pause process
//////

// shouldTripCircuitBreaker returns true if any circuit breaker is above its
// threshold value. The thresholds are defined in the `thresholds` map.
// The function returns false if the breaker is not in the map or if the usage
// is below the threshold.
func shouldTripCircuitBreaker(metrics IndexMetrics, thresholds map[string]float64) bool {
	for breaker, usage := range metrics.CircuitBreakers {
		threshold, exists := thresholds[breaker]

		if exists && usage > threshold {
			return true
		}
	}

	return false
}

// shouldTemporaryPause returns true if any metric is above its threshold value.
func shouldTemporaryPause[T any](metrics IndexMetrics, opts BulkOptions[T]) bool {
	return metrics.JVMHeapUsage > opts.MaxJVMHeapUsage ||
		metrics.IndexingPressure > opts.MaxIndexingPressure ||
		metrics.UnassignedShards > 0 ||
		shouldTripCircuitBreaker(metrics, opts.CircuitBreakerThresholds)
}

//////
// Refresh process.
//////

// Refresh the index to free up translog space.
func refreshIndex(ctx context.Context, client *elasticsearch.Client, index string) error {
	res, err := client.Indices.Refresh(
		client.Indices.Refresh.WithContext(ctx),
		client.Indices.Refresh.WithIndex(index),
	)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to refresh index: %s", res.String())
	}

	return nil
}

//////
// Bulk options process.
//////

// RoundToNearestThousandDivisibleBy2 rounds up to the nearest thousand and
// then to the nearest even number divisible by 2.
func roundToNearestThousandDivisibleBy2(n int) int {
	// Round up to nearest thousand.
	thousand := ((n + 999) / 1000) * 1000

	// If not divisible by 2000, round up to next 2000.
	if thousand%2000 != 0 {
		thousand = ((thousand + 1999) / 2000) * 2000
	}

	return thousand
}

// NewBulkOptions calculates the bulk indexing options based on the
// sample document size and cluster configuration.
func NewBulkOptions[T any](
	// Index name.
	indexName string,

	// Sample document.
	sampleDoc json.RawMessage,

	// Cluster configuration.
	numDataNodes, ramPerNodeGB int,

	// Refresh policy.
	shouldRefresh bool,

	// Optional functions.
	indexNameFunc func(indexName string) string,
	documentIDFunc func(doc T) string,
	routingFunc func(doc T) string,
) BulkOptions[T] {
	refreshPolicy := "false"

	if shouldRefresh {
		refreshPolicy = "wait_for"
	}

	// Calculate the size of the sample document.
	docSize := len(sampleDoc) // in bytes.

	// Determine the recommended batch size.
	targetBatchSizeBytes := 5 * 1024 * 1024 // Target batch size of 5MB.

	recommendedBatchSize := targetBatchSizeBytes / docSize

	if recommendedBatchSize < 1 {
		recommendedBatchSize = 1
	}

	// Calculate FlushBytes based on batch size and document size.
	FlushBytes := recommendedBatchSize * docSize

	// Determine the number of workers per node. Assuming JVM heap size is half
	// of RAM per node.
	jvmHeapSizeGB := float64(ramPerNodeGB) / 2

	// Assume each worker uses approximately 0.5GB of heap.
	workersPerNode := int(jvmHeapSizeGB / 1)

	if workersPerNode < 1 {
		workersPerNode = 1
	}

	// Calculate the total recommended number of workers.
	recommendedWorkers := numDataNodes * workersPerNode

	// Set MaxJVMHeapUsage as a fraction of JVM heap size.
	MaxJVMHeapUsage := 0.75

	// Calculate MaxTranslogSize. Allow space for two batches.
	MaxTranslogSize := int64(float64(FlushBytes) * 100)

	// Define Circuit Breaker Thresholds.
	CircuitBreakerThresholds := map[string]float64{
		"parent":     0.70,
		"fielddata":  0.80,
		"request":    0.90,
		"accounting": 0.85,
	}

	return BulkOptions[T]{
		BatchSize:  roundToNearestThousandDivisibleBy2(recommendedBatchSize),
		NumWorkers: recommendedWorkers,

		DocumentIDFunc: documentIDFunc,
		IndexNameFunc:  indexNameFunc,
		RoutingFunc:    routingFunc,

		CircuitBreakerThresholds: CircuitBreakerThresholds,

		FlushBytes:          FlushBytes,
		FlushInterval:       30 * time.Second,
		Index:               indexName,
		MaxIndexingPressure: 0.70,
		MaxJVMHeapUsage:     MaxJVMHeapUsage,
		MaxTranslogSize:     MaxTranslogSize,
		MetricsCheck:        5 * time.Second,
		RefreshPolicy:       refreshPolicy,
		RetryOnFailure:      3,
	}
}
