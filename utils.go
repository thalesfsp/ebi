package ebi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/thalesfsp/customerror"
)

//////
// Update metrics process.
//////

// updateIndexMetrics fetches index and node stats from Elasticsearch and
// updates the provided IndexMetrics struct.
//
//nolint:forcetypeassert
func updateIndexMetrics(ctx context.Context, client *elasticsearch.Client, metrics *IndexMetrics) error {
	// Get index stats.
	indexStats, err := client.Indices.Stats(
		client.Indices.Stats.WithContext(ctx),
		client.Indices.Stats.WithMetric("segments", "indexing", "translog"),
	)
	if err != nil {
		return customerror.NewFailedToError("get index stats", customerror.WithError(err))
	}

	defer indexStats.Body.Close()

	// Get nodes stats for JVM and circuit breakers.
	nodesStats, err := client.Nodes.Stats(
		client.Nodes.Stats.WithContext(ctx),
		client.Nodes.Stats.WithMetric("jvm", "breaker"),
	)
	if err != nil {
		return customerror.NewFailedToError("get nodes stats", customerror.WithError(err))
	}

	defer nodesStats.Body.Close()

	// Parse index stats.
	var indexStatsResp map[string]interface{}

	if err := json.NewDecoder(indexStats.Body).Decode(&indexStatsResp); err != nil {
		return customerror.NewFailedToError("parse index stats", customerror.WithError(err))
	}

	// Parse nodes stats.
	var nodesStatsResp map[string]interface{}

	if err := json.NewDecoder(nodesStats.Body).Decode(&nodesStatsResp); err != nil {
		return customerror.NewFailedToError("parse nodes stats", customerror.WithError(err))
	}

	// Extract metrics from responses.
	indices := indexStatsResp["indices"].(map[string]interface{})

	for _, indexData := range indices {
		data := indexData.(map[string]interface{})

		// Get segments info.
		if segments, ok := data["segments"].(map[string]interface{}); ok {
			metrics.SegmentsCount = int(segments["count"].(float64))

			metrics.SegmentsMemory = int64(segments["memory_in_bytes"].(float64))
		}

		// Get translog info.
		if translog, ok := data["translog"].(map[string]interface{}); ok {
			metrics.TranslogSize = int64(translog["size_in_bytes"].(float64))
		}
	}

	// Extract node metrics.
	nodes := nodesStatsResp["nodes"].(map[string]interface{})

	var totalHeap, usedHeap float64

	for _, nodeData := range nodes {
		data := nodeData.(map[string]interface{})

		// Get JVM info.
		if jvm, ok := data["jvm"].(map[string]interface{}); ok {
			if mem, ok := jvm["mem"].(map[string]interface{}); ok {
				totalHeap += mem["heap_max_in_bytes"].(float64)

				usedHeap += mem["heap_used_in_bytes"].(float64)
			}
		}

		// Get circuit breaker info.
		if breakers, ok := data["breakers"].(map[string]interface{}); ok {
			for name, breakerData := range breakers {
				breaker := breakerData.(map[string]interface{})

				metrics.CircuitBreakers[name] = breaker["estimated_size_in_bytes"].(float64) /
					breaker["limit_size_in_bytes"].(float64)
			}
		}
	}

	metrics.JVMHeapUsage = usedHeap / totalHeap

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
