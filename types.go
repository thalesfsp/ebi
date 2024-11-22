package ebi

import "time"

//////
// Const, vars, and types.
//////

// BulkOptions defines the options for bulk indexing.
type BulkOptions[T any] struct {
	// These are options that can be used in the hyperparameter tuning.
	BatchSize  int `json:"batch_size"  validate:"required"`
	NumWorkers int `json:"num_workers" validate:"required"`

	// MetricsCheck determines how often the metrics are checked and sent via
	// channel.
	MetricsCheck time.Duration `default:"5s" json:"metrics_check" validate:"required"`

	CircuitBreakerThresholds map[string]float64

	FlushBytes          int
	FlushInterval       time.Duration
	Index               string
	MaxIndexingPressure float64
	MaxJVMHeapUsage     float64
	MaxTranslogSize     int64
	RefreshPolicy       string
	RetryOnFailure      int

	//////
	// Dynamic options, they are optional.
	//////

	// DocumentIDFunc determines in the evaluation time the document ID.
	DocumentIDFunc func(doct T) string

	// IndexNameFunc determines in the evaluation time the index name.
	IndexNameFunc func(indexName string) string

	// RoutingFunc determines in the evaluation time the routing value.
	RoutingFunc func(doct T) string
}

//////
// Bulk metrics.
//////

// FailedItem contains information about a document that failed to index.
type FailedItem struct {
	ID     string
	Reason string
	Status int
}

// BulkMetrics contains metrics for a bulk indexing operation.
type BulkMetrics struct {
	BytesProcessed int64
	DocsFailed     int64
	DocsProcessed  int64
	DocsSucceeded  int64
	Error          error
	ErrorCount     int64
	FailedItems    []FailedItem
	LastError      error
}

//////
// Index metrics.
//////

// IndexMetrics contains metrics for an Elasticsearch index.
type IndexMetrics struct {
	CircuitBreakers  map[string]float64
	IndexingPressure float64
	JVMHeapUsage     float64
	SegmentsCount    int
	SegmentsMemory   int64
	TranslogSize     int64
	UnassignedShards int
}

//////
// Total metrics.
//////

// GlobalMetrics contains metrics for a bulk indexing operation and an ES index.
type GlobalMetrics struct {
	*BulkMetrics

	*IndexMetrics
}
