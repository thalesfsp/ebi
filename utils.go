package ebi

import (
	"context"
	"encoding/json"
	"math"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/thalesfsp/configurer/util"
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
func calculateSystemPressure(
	metrics *Metrics,
	nodeStats NodesStats,
) {
	var (
		memoryPressurePoints float64
		totalMemoryFactors   float64

		writePressurePoints float64
		totalWriteFactors   float64

		memoryPressure  float64
		writePressure   float64
		overallPressure float64
	)

	var (
		dataNodes  int64
		totalRAMGB int64
	)

	// Calculated memory pressure based on:
	// - JVM heap usage
	// - Circuit breaker status
	// - Indexing pressure memory
	for _, node := range nodeStats.Nodes {
		//////
		// Data nodes and total RAM.
		//////

		if strings.Contains(strings.Join(node.Roles, ","), "data") {
			dataNodes++

			totalRAMGB += node.JVM.Mem.HeapMaxInBytes / 1024 / 1024 / 1024
		}

		if node.JVM != nil && node.JVM.Mem != nil {
			// JVM heap contributes 40% of the memory pressure.
			memoryPressurePoints += float64(node.JVM.Mem.HeapUsedPercent) * 0.4
			totalMemoryFactors += 40
		}

		if node.Breakers != nil && node.Breakers.Parent != nil {
			// Circuit breaker contributes 30% of the memory pressure.
			breakerPressure := float64(node.Breakers.Parent.EstimatedSizeInBytes) /
				float64(node.Breakers.Parent.LimitSizeInBytes) * 100
			memoryPressurePoints += breakerPressure * 0.3
			totalMemoryFactors += 30
		}

		// Indexing pressure memory contributes 30% of the memory pressure.
		if pressure := node.IndexingPressure.Memory; pressure != nil {
			if pressure.LimitInBytes > 0 {
				currentPressure := float64(pressure.Current.AllInBytes) /
					float64(pressure.LimitInBytes) * 100
				memoryPressurePoints += currentPressure * 0.3
				totalMemoryFactors += 30
			}
		}

		// Calculate write pressure based on:
		// - Write load
		// - Throttle status
		// - Merge pressure
		if node.Indices != nil {
			// Write load contributes 50% of the write pressure.
			if writeLoad := node.Indices.Indexing.WriteLoad; writeLoad > 0 {
				// Normalize the write load (assume that >1 is 100% pressure).
				writePressure := math.Min(writeLoad*100, 100)

				writePressurePoints += writePressure * 0.5

				totalWriteFactors += 50
			}

			// Merge status contributes 30% of the write pressure.
			if merges := node.Indices.Merges; merges != nil && merges.TotalTimeInMillis > 0 {
				// If spending more than 10% of the time in merges, consider pressure.
				mergePressure := math.Min(
					float64(merges.TotalTimeInMillis)/
						float64(node.JVM.UptimeInMillis)*1000, 100,
				)

				writePressurePoints += mergePressure * 0.3

				totalWriteFactors += 30
			}

			// Throttle status contributes 20% of the write pressure.
			if node.Indices.Indexing.IsThrottled {
				writePressurePoints += 100 * 0.2

				totalWriteFactors += 20
			}
		}
	}

	// Normalize the memory pressure.
	if totalMemoryFactors > 0 {
		memoryPressure = memoryPressurePoints / totalMemoryFactors * 100
	}

	// Normalize the write pressure.
	if totalWriteFactors > 0 {
		writePressure = writePressurePoints / totalWriteFactors * 100
	}

	// Overall Pressure is the composition of memory (60%) and write (40%).
	overallPressure = (memoryPressure * 0.6) + (writePressure * 0.4)

	// Update metrics.
	metrics.UpdateMemoryPressure(memoryPressure)

	metrics.UpdateWritePressure(writePressure)

	metrics.UpdateOverallPressure(overallPressure)

	// Calculate average RAM per node.
	if dataNodes > 0 {
		metrics.numDataNodes = int(dataNodes)

		metrics.ramPerNodeGB = int(totalRAMGB / dataNodes)
	}
}

// updateIndexMetrics fetches node stats from Elasticsearch and
// updates the provided IndexMetrics struct.
func updateIndexMetrics(
	ctx context.Context,
	client *elasticsearch.Client,
	metrics *Metrics,
) error {
	//////
	// Nodes stats.
	//////

	nodesStats, err := client.Nodes.Stats(
		client.Nodes.Stats.WithContext(ctx),
		client.Nodes.Stats.WithMetric("jvm", "breaker", "indexing_pressure", "indices"),
	)
	if err != nil {
		return customerror.NewFailedToError("get nodes stats", customerror.WithError(err))
	}

	defer nodesStats.Body.Close()

	var nodesStatsResp NodesStats
	if err := json.NewDecoder(nodesStats.Body).Decode(&nodesStatsResp); err != nil {
		return customerror.NewFailedToError("parse nodes stats", customerror.WithError(err))
	}

	// Calculate, and update system pressure metrics.
	calculateSystemPressure(metrics, nodesStatsResp)

	// Nodes stats.
	metrics.NodeStats = &nodesStatsResp

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

// Process `v`:
// - Set default values using the `default` field tag.
// - Set values from environment variables using the `env` field tag.
// - Validating the values using the `validate` field tag.
//
// Order of operations:
// 1. Set default values
// 2. Set values from environment variables
// 3. Validate values
//
// NOTE: `v` must be a pointer to a struct.
// NOTE: It only sets default values for fields that are not set.
// NOTE: It'll set the value from env vars even if it's not empty (precedence).
//
// NOTE: Like the built-in `json` tag, it'll ignore the field if it isn't
// exported, and if tag is set to `-`.
func process(v any) error {
	return util.Process(v)
}
