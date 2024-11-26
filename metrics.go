package ebi

import (
	"sync"

	"github.com/thalesfsp/customerror"
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

// UpdateBytesProcessed updates the number of bytes processed.
func (gm *Metrics) UpdateBytesProcessed(delta int64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.BytesProcessed += delta
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
		return nil, customerror.NewInvalidError("metrics", customerror.WithError(err))
	}

	return m, nil
}
