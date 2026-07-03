package ebi

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests pin calculateSystemPressure against partially-populated node
// stats. Elasticsearch omits whole sections (jvm, breakers,
// indexing_pressure, indices.indexing, ...) depending on node role, version
// and the requested metric filter — every pointer in NodeDetails can
// legitimately be nil, and every limit can legitimately be 0. The function
// must never panic and must never emit NaN/Inf pressure numbers: a single
// poisoned node would silently corrupt the pause/refresh decisions for the
// whole bulk run.

// fullStatsNode returns a node with every section populated, chosen so the
// expected pressure numbers are exact:
//
//	memory: heap 50%*0.4=20pts(+40) + breaker 50%*0.3=15pts(+30) +
//	        indexing-pressure 10%*0.3=3pts(+30) => 38/100*100 = 38
//	write:  writeLoad 0.5→50*0.5=25pts(+50) + merges 100/10000*1000=10*0.3=3pts(+30) +
//	        throttled 100*0.2=20pts(+20) => 48/100*100 = 48
//	overall: 38*0.6 + 48*0.4 = 42
func fullStatsNode() *NodeDetails {
	return &NodeDetails{
		Roles: []string{"data", "master"},
		JVM: &JVMStats{
			UptimeInMillis: 10_000,
			Mem: &JVMMemoryStats{
				HeapUsedPercent: 50,
				HeapMaxInBytes:  8 * 1024 * 1024 * 1024,
			},
		},
		Breakers: &BreakerStats{
			Parent: &BreakerDetails{
				EstimatedSizeInBytes: 50,
				LimitSizeInBytes:     100,
			},
		},
		IndexingPressure: &IndexingPressureStats{
			Memory: &IndexingPressureMemory{
				Current:      &IndexingPressureMemoryUsage{AllInBytes: 10},
				LimitInBytes: 100,
			},
		},
		Indices: &IndicesStats{
			Indexing: &IndexingStats{
				WriteLoad:   0.5,
				IsThrottled: true,
			},
			Merges: &MergesStats{
				TotalTimeInMillis: 100,
			},
		},
	}
}

//nolint:maintidx
func TestCalculateSystemPressure_Table(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		nodes map[string]*NodeDetails

		// Exact expectations; negative values mean "only assert sanity"
		// (non-NaN, non-Inf, within 0..100).
		wantMemory  float64
		wantWrite   float64
		wantOverall float64

		wantNumDataNodes int
		wantRAMPerNodeGB int
	}{
		{
			name:             "full stats exact numbers",
			nodes:            map[string]*NodeDetails{"n1": fullStatsNode()},
			wantMemory:       38,
			wantWrite:        48,
			wantOverall:      42,
			wantNumDataNodes: 1,
			wantRAMPerNodeGB: 8,
		},
		{
			name: "data-role node with nil JVM",
			nodes: map[string]*NodeDetails{"n1": {
				Roles: []string{"data"},
				JVM:   nil,
			}},
			wantMemory:       0,
			wantWrite:        0,
			wantOverall:      0,
			wantNumDataNodes: 1,
			wantRAMPerNodeGB: 0,
		},
		{
			name: "data-role node with nil JVM.Mem",
			nodes: map[string]*NodeDetails{"n1": {
				Roles: []string{"data"},
				JVM:   &JVMStats{UptimeInMillis: 1000, Mem: nil},
			}},
			wantMemory:       0,
			wantWrite:        0,
			wantOverall:      0,
			wantNumDataNodes: 1,
			wantRAMPerNodeGB: 0,
		},
		{
			name: "nil IndexingPressure section",
			nodes: map[string]*NodeDetails{"n1": {
				Roles: []string{"ingest"},
				JVM: &JVMStats{
					UptimeInMillis: 1000,
					Mem:            &JVMMemoryStats{HeapUsedPercent: 50},
				},
				IndexingPressure: nil,
			}},
			// Heap is the only factor: 20pts/40 => 50.
			wantMemory:       50,
			wantWrite:        0,
			wantOverall:      30,
			wantNumDataNodes: 0,
			wantRAMPerNodeGB: 0,
		},
		{
			name: "IndexingPressure.Memory with nil Current",
			nodes: map[string]*NodeDetails{"n1": {
				Roles: []string{"ingest"},
				IndexingPressure: &IndexingPressureStats{
					Memory: &IndexingPressureMemory{
						Current:      nil,
						LimitInBytes: 100,
					},
				},
			}},
			wantMemory:       0,
			wantWrite:        0,
			wantOverall:      0,
			wantNumDataNodes: 0,
			wantRAMPerNodeGB: 0,
		},
		{
			name: "Indices with nil Indexing",
			nodes: map[string]*NodeDetails{"n1": {
				Roles: []string{"ingest"},
				Indices: &IndicesStats{
					Indexing: nil,
				},
			}},
			wantMemory:       0,
			wantWrite:        0,
			wantOverall:      0,
			wantNumDataNodes: 0,
			wantRAMPerNodeGB: 0,
		},
		{
			name: "merges present but nil JVM",
			nodes: map[string]*NodeDetails{"n1": {
				Roles: []string{"ingest"},
				JVM:   nil,
				Indices: &IndicesStats{
					Indexing: &IndexingStats{},
					Merges:   &MergesStats{TotalTimeInMillis: 500},
				},
			}},
			// The merges factor needs JVM uptime as the denominator; with
			// JVM absent it must be skipped, not divide by nil/zero.
			wantMemory:       0,
			wantWrite:        0,
			wantOverall:      0,
			wantNumDataNodes: 0,
			wantRAMPerNodeGB: 0,
		},
		{
			name: "merges present but zero JVM uptime",
			nodes: map[string]*NodeDetails{"n1": {
				Roles: []string{"ingest"},
				JVM:   &JVMStats{UptimeInMillis: 0},
				Indices: &IndicesStats{
					Indexing: &IndexingStats{},
					Merges:   &MergesStats{TotalTimeInMillis: 500},
				},
			}},
			wantMemory:       0,
			wantWrite:        0,
			wantOverall:      0,
			wantNumDataNodes: 0,
			wantRAMPerNodeGB: 0,
		},
		{
			name: "breaker LimitSizeInBytes zero",
			nodes: map[string]*NodeDetails{"n1": {
				Roles: []string{"ingest"},
				JVM: &JVMStats{
					UptimeInMillis: 1000,
					Mem:            &JVMMemoryStats{HeapUsedPercent: 50},
				},
				Breakers: &BreakerStats{
					Parent: &BreakerDetails{
						EstimatedSizeInBytes: 100,
						LimitSizeInBytes:     0,
					},
				},
			}},
			// A zero breaker limit must be skipped: dividing by it yields
			// +Inf which poisons every downstream pressure number. Heap is
			// then the only factor: 50.
			wantMemory:       50,
			wantWrite:        0,
			wantOverall:      30,
			wantNumDataNodes: 0,
			wantRAMPerNodeGB: 0,
		},
		{
			name: "indexing pressure LimitInBytes zero",
			nodes: map[string]*NodeDetails{"n1": {
				Roles: []string{"ingest"},
				IndexingPressure: &IndexingPressureStats{
					Memory: &IndexingPressureMemory{
						Current:      &IndexingPressureMemoryUsage{AllInBytes: 10},
						LimitInBytes: 0,
					},
				},
			}},
			wantMemory:       0,
			wantWrite:        0,
			wantOverall:      0,
			wantNumDataNodes: 0,
			wantRAMPerNodeGB: 0,
		},
		{
			name:             "empty nodes map",
			nodes:            map[string]*NodeDetails{},
			wantMemory:       0,
			wantWrite:        0,
			wantOverall:      0,
			wantNumDataNodes: 0,
			wantRAMPerNodeGB: 0,
		},
		{
			// A `"nodes": {"x": null}` payload decodes into a nil entry.
			name:             "nil node entry",
			nodes:            map[string]*NodeDetails{"n1": nil, "n2": fullStatsNode()},
			wantMemory:       38,
			wantWrite:        48,
			wantOverall:      42,
			wantNumDataNodes: 1,
			wantRAMPerNodeGB: 8,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			metrics, err := NewMetrics()
			require.NoError(t, err)

			// Must be panic-free for every partially-populated shape.
			require.NotPanics(t, func() {
				calculateSystemPressure(metrics, NodesStats{Nodes: tc.nodes})
			}, "calculateSystemPressure must not panic on partially-populated node stats")

			got := metrics.GetMetrics()

			// Sanity for every case: no NaN/Inf, all pressures in 0..100.
			for name, v := range map[string]float64{
				"MemoryPressure":  got.MemoryPressure,
				"WritePressure":   got.WritePressure,
				"OverallPressure": got.OverallPressure,
			} {
				assert.False(t, math.IsNaN(v), "%s must not be NaN", name)
				assert.False(t, math.IsInf(v, 0), "%s must not be Inf", name)
				assert.GreaterOrEqual(t, v, 0.0, "%s must be >= 0", name)
				assert.LessOrEqual(t, v, 100.0, "%s must be <= 100", name)
			}

			assert.InDelta(t, tc.wantMemory, got.MemoryPressure, 1e-9, "MemoryPressure")
			assert.InDelta(t, tc.wantWrite, got.WritePressure, 1e-9, "WritePressure")
			assert.InDelta(t, tc.wantOverall, got.OverallPressure, 1e-9, "OverallPressure")

			assert.Equal(t, tc.wantNumDataNodes, got.numDataNodes, "numDataNodes")
			assert.Equal(t, tc.wantRAMPerNodeGB, got.ramPerNodeGB, "ramPerNodeGB")
		})
	}
}
