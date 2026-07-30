package ebi

import (
	"sync"

	"github.com/thalesfsp/customerror/v2"
)

//////
// Const, vars, and types.
//////

// Status represents the status of a bulk indexing operation.
type Status = string

const (
	// StatusPaused represents a paused status.
	StatusPaused Status = "paused"

	// StatusRunning represents a running status.
	StatusRunning Status = "running"

	// StatusDone represents a done status.
	StatusDone Status = "done"
)

// Metrics contains metrics for a bulk indexing operation and an ES index.
//
// WARN: Changes here requires changes in GetMetrics(), Update* and NewMetrics()
// functions.
//
// NOTE: Use NewMetrics() to create a new Metrics struct!
type Metrics struct {
	// Status of the process.
	Status Status `json:"status"`

	// Bulk metrics.
	BytesProcessed int64 `json:"bytesProcessed"`
	DocsFailed     int64 `json:"docsFailed"`
	DocsProcessed  int64 `json:"docsProcessed"`
	DocsSucceeded  int64 `json:"docsSucceeded"`
	ErrorCount     int64 `json:"errorCount"`

	//////
	// Overall metrics.
	//////

	// Overall pressure (0-100%).
	OverallPressure float64 `json:"overallPressure"`

	// Memory pressure (0-100%).
	MemoryPressure float64 `json:"memoryPressure"`

	// Write pressure (0-100%).
	WritePressure float64 `json:"writePressure"`

	//////
	// Nodes.
	//////

	// NodeStats contains all node stats.
	NodeStats *NodesStats `json:"nodeStats"`

	numDataNodes int `json:"-"`
	ramPerNodeGB int `json:"-"`

	mu sync.Mutex `json:"-"`
}

//////
// Methods.
//////

// UpdateStatus updates the status.
func (gm *Metrics) UpdateStatus(status string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.Status = status
}

// UpdateOverallPressure updates the overall pressure.
func (gm *Metrics) UpdateOverallPressure(overallPressure float64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.OverallPressure = overallPressure
}

// UpdateMemoryPressure updates the memory pressure.
func (gm *Metrics) UpdateMemoryPressure(memoryPressure float64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.MemoryPressure = memoryPressure
}

// UpdateWritePressure updates the write pressure.
func (gm *Metrics) UpdateWritePressure(writePressure float64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.WritePressure = writePressure
}

// updateComputed publishes every derived value in ONE critical section so a
// concurrent GetMetrics can never observe a half-updated set (e.g. a new
// MemoryPressure beside a stale OverallPressure).
//
// numDataNodes/ramPerNodeGB are applied only when numDataNodes > 0: a stats
// response that reports no data nodes must not wipe the last known topology,
// which BulkCreate relies on to size its worker pool.
func (gm *Metrics) updateComputed(
	memoryPressure, writePressure, overallPressure float64,
	numDataNodes, ramPerNodeGB int,
) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.MemoryPressure = memoryPressure
	gm.WritePressure = writePressure
	gm.OverallPressure = overallPressure

	if numDataNodes > 0 {
		gm.numDataNodes = numDataNodes
		gm.ramPerNodeGB = ramPerNodeGB
	}
}

// UpdateBytesProcessed updates the number of bytes processed.
func (gm *Metrics) UpdateBytesProcessed(delta int64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.BytesProcessed += delta
}

// AddProcessed records one processed document and the bytes it contributed in a
// SINGLE critical section. The hot path always updates both together, so taking
// the mutex twice (IncreaseDocsProcessed + UpdateBytesProcessed) is pure
// overhead. bytes may be 0 — delete operations carry no body.
func (gm *Metrics) AddProcessed(bytes int64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.DocsProcessed++
	gm.BytesProcessed += bytes
}

// IncreaseDocsFailed increases the number of documents that failed.
func (gm *Metrics) IncreaseDocsFailed() {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.DocsFailed++
}

// IncreaseDocsProcessed increases the number of documents processed.
func (gm *Metrics) IncreaseDocsProcessed() {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.DocsProcessed++
}

// IncreaseDocsSucceeded increases the number of documents that succeeded.
func (gm *Metrics) IncreaseDocsSucceeded() {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.DocsSucceeded++
}

// IncrementErrorCount increments the error count.
func (gm *Metrics) IncrementErrorCount() {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.ErrorCount++
}

// UpdateNodeStats replaces the node stats.
func (gm *Metrics) UpdateNodeStats(ns *NodesStats) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.NodeStats = ns
}

// getNodeInfo returns the node topology snapshot (numDataNodes,
// ramPerNodeGB). Locked counterpart of updateComputed.
func (gm *Metrics) getNodeInfo() (int, int) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	return gm.numDataNodes, gm.ramPerNodeGB
}

// getStatus returns the status under lock. The per-document pause check in
// BulkCreate uses this instead of GetMetrics() so reading one field does not
// allocate and copy the whole struct.
func (gm *Metrics) getStatus() Status {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	return gm.Status
}

// GetMetrics returns a copy of the Metrics struct.
func (gm *Metrics) GetMetrics() *Metrics {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	return &Metrics{
		Status: gm.Status,

		BytesProcessed: gm.BytesProcessed,
		DocsFailed:     gm.DocsFailed,
		DocsProcessed:  gm.DocsProcessed,
		DocsSucceeded:  gm.DocsSucceeded,
		ErrorCount:     gm.ErrorCount,

		OverallPressure: gm.OverallPressure,
		MemoryPressure:  gm.MemoryPressure,
		WritePressure:   gm.WritePressure,

		NodeStats: gm.NodeStats,

		numDataNodes: gm.numDataNodes,
		ramPerNodeGB: gm.ramPerNodeGB,
	}
}

//////
// Factory.
//////

// NewMetrics creates a new Metrics struct.
func NewMetrics() (*Metrics, error) {
	m := &Metrics{
		Status: StatusRunning,

		BytesProcessed: 0,
		DocsFailed:     0,
		DocsProcessed:  0,
		DocsSucceeded:  0,
		ErrorCount:     0,

		OverallPressure: 0,
		MemoryPressure:  0,
		WritePressure:   0,

		NodeStats: nil,

		numDataNodes: 0,
		ramPerNodeGB: 0,

		mu: sync.Mutex{},
	}

	if err := process(m); err != nil {
		return nil, ErrorCatalog.
			MustGet(ErrInvalidMetrics).
			NewInvalidError(customerror.WithError(err))
	}

	return m, nil
}
