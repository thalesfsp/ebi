package ebi

import (
	"context"
	"encoding/json"
	"time"

	"github.com/thalesfsp/customerror"
)

//////
// Const, vars, and types.
//////

// BulkOptionsFunc allows to set options.
type BulkOptionsFunc[T any] func(o *BulkOptions[T])

// RefreshPolicy defines the refresh policy for the bulk indexing operation.
type RefreshPolicy = string

const (
	// RefreshPolicyFalse is the default refresh policy, no refresh is forced
	// after a operation.
	RefreshPolicyFalse RefreshPolicy = "false"

	// RefreshPolicyImmediate forces an immediate refresh after a operation.
	RefreshPolicyImmediate RefreshPolicy = "immediate"

	// RefreshPolicyWaitFor waits for a refresh before completing the operation.
	RefreshPolicyWaitFor RefreshPolicy = "wait_for"
)

// NumWorkersFunc calculates the number of workers based on the cluster configuration.
type NumWorkersFunc func() (int, error)

// NumWorkersByJVM calculates the number of workers based on the JVM heap size.
func NumWorkersByJVM(metrics *Metrics) NumWorkersFunc {
	return func() (int, error) {
		jvmHeapSizeGB := float64(metrics.ramPerNodeGB) / 2

		workersPerNode := int(jvmHeapSizeGB / 1)

		if workersPerNode < 1 {
			workersPerNode = 1
		}

		return workersPerNode, nil
	}
}

// NumWorkersManual sets the number of workers manually.
func NumWorkersManual(n int) NumWorkersFunc {
	return func() (int, error) {
		return n, nil
	}
}

// NumWorkersAutoDiscovery calls ElasticSearch to discover the number of workers
// based on the cluster.
func NumWorkersAutoDiscovery[T any](ctx context.Context, ebi *EBI[T]) NumWorkersFunc {
	return func() (int, error) {
		n, err := ebi.discoverWokerNodes(ctx)
		if err != nil {
			return 0, err
		}

		return n, nil
	}
}

// BulkOptions defines the options for bulk indexing.
//
// NOTE: Use NewBulkOptions() to create a new BulkOptions struct!
//
//nolint:lll
type BulkOptions[T any] struct {
	// These are options that can be used in the hyperparameter tuning.
	BatchSize  int            `json:"batchSize"`
	NumWorkers NumWorkersFunc `json:"numWorkers"`

	// Sample doc.
	SampleDoc json.RawMessage `json:"sampleDoc" validate:"required"`

	// Metrics related.
	MetricsCheck time.Duration `default:"5s" json:"metricsCheck" validate:"required"`

	MetricsCh chan<- *Metrics

	// Async error handling.
	ErrorCh chan<- error

	// Internals related.
	FlushBytes     int           `json:"flushBytes" validate:"omitempty,gt=0"`
	FlushInterval  time.Duration `default:"30s"     json:"flushInterval"      validate:"omitempty,gt=0"`
	Index          string        `json:"index"      validate:"required"`
	RefreshPolicy  RefreshPolicy `default:"false"   json:"refreshPolicy"      validate:"omitempty,oneof=false immediate wait_for"`
	RetryOnFailure int           `default:"3"       json:"retryOnFailure"     validate:"omitempty,gte=0"`

	//////
	// Dynamic options, they are optional.
	//////

	// DocumentIDFunc determines in the evaluation time the document ID.
	DocumentIDFunc func(doct T) string `json:"-"`

	// FlushEndFunc do something when flushing ends.
	FlushEndFunc func(ctx context.Context) `json:"-"`

	// FlushStartFunc do something when flushing starts.
	FlushStartFunc func(ctx context.Context) context.Context `json:"-"`

	// IndexNameFunc determines in the evaluation time the index name.
	IndexNameFunc func(indexName string) string `json:"-"`

	// RoutingFunc determines in the evaluation time the routing value.
	RoutingFunc func(doct T) string `json:"-"`

	// PauseFunc determines the conditions to pause the indexing process.
	PauseFunc PauseFunc `json:"-"`

	// PauseDuration determines the duration to pause the indexing process.
	PauseDuration time.Duration `default:"5s" json:"pauseDuration" validate:"omitempty,gt=0"`

	// RefreshFunc determines the conditions to refresh the index.
	RefreshFunc RefreshFunc `json:"-"`
}

//////
// Exported built-in options.
//////

// WithMetricsCheck sets the metrics check for the bulk indexing operation.
func WithMetricsCheck[T any](metricsCheck time.Duration) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.MetricsCheck = metricsCheck
	}
}

// WithMetricsCh sets the metrics channel for the bulk indexing operation.
func WithMetricsCh[T any](metricsCh chan<- *Metrics) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.MetricsCh = metricsCh
	}
}

// WithErrorCh sets the error channel for the bulk indexing operation.
func WithErrorCh[T any](errorCh chan<- error) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.ErrorCh = errorCh
	}
}

// WithRefreshPolicy sets the refresh policy for the bulk indexing operation.
func WithRefreshPolicy[T any](refreshPolicy RefreshPolicy) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.RefreshPolicy = refreshPolicy
	}
}

// WithIndexNameFunc sets the index name function for the bulk indexing operation.
func WithIndexNameFunc[T any](indexNameFunc func(indexName string) string) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.IndexNameFunc = indexNameFunc
	}
}

// WithDocumentIDFunc sets the document ID function for the bulk indexing operation.
func WithDocumentIDFunc[T any](documentIDFunc func(doc T) string) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.DocumentIDFunc = documentIDFunc
	}
}

// WithRoutingFunc sets the routing function for the bulk indexing operation.
func WithRoutingFunc[T any](routingFunc func(doc T) string) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.RoutingFunc = routingFunc
	}
}

// WithPauseFunc sets the pause function for the bulk indexing operation.
func WithPauseFunc[T any](pauseFunc PauseFunc) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.PauseFunc = pauseFunc
	}
}

// WithPauseDuration sets the pause duration for the bulk indexing operation.
func WithPauseDuration[T any](pauseDuration time.Duration) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.PauseDuration = pauseDuration
	}
}

// WithRefreshFunc sets the refresh function for the bulk indexing operation.
func WithRefreshFunc[T any](refreshFunc RefreshFunc) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.RefreshFunc = refreshFunc
	}
}

// WithBatchSize sets the batch size for the bulk indexing operation.
func WithBatchSize[T any](batchSize int) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.BatchSize = batchSize
	}
}

// WithFlushBytes sets the flush bytes for the bulk indexing operation.
func WithFlushBytes[T any](flushBytes int) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.FlushBytes = flushBytes
	}
}

// WithFlushInterval sets the flush interval for the bulk indexing operation.
func WithFlushInterval[T any](flushInterval time.Duration) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.FlushInterval = flushInterval
	}
}

// WithRetryOnFailure sets the retry on failure for the bulk indexing operation.
func WithRetryOnFailure[T any](retryOnFailure int) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.RetryOnFailure = retryOnFailure
	}
}

// WithNumWorkers sets the number of workers for the bulk indexing operation.
func WithNumWorkers[T any](numWorkers NumWorkersFunc) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.NumWorkers = numWorkers
	}
}

// WithFlushEndFunc sets the flush end function for the bulk indexing operation.
func WithFlushEndFunc[T any](flushEndFunc func(ctx context.Context)) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.FlushEndFunc = flushEndFunc
	}
}

// WithFlushStartFunc sets the flush start function for the bulk indexing operation.
func WithFlushStartFunc[T any](flushStartFunc func(ctx context.Context) context.Context) BulkOptionsFunc[T] {
	return func(o *BulkOptions[T]) {
		o.FlushStartFunc = flushStartFunc
	}
}

//////
// Factory.
//////

// NewBulkOptions calculates the bulk indexing options based on the
// sample document size and cluster configuration.
func NewBulkOptions[T any](
	// Index name.
	indexName string,

	// Sample document.
	sampleDoc json.RawMessage,

	options ...BulkOptionsFunc[T],
) (*BulkOptions[T], error) {
	//////
	// Apply options.
	//////

	var opts BulkOptions[T]

	// Iterate over the options and apply them against params.
	for _, option := range options {
		option(&opts)
	}

	bO := &BulkOptions[T]{
		Index:     indexName,
		SampleDoc: sampleDoc,

		RefreshPolicy: opts.RefreshPolicy,

		BatchSize:      opts.BatchSize,
		ErrorCh:        opts.ErrorCh,
		FlushBytes:     opts.FlushBytes,
		FlushInterval:  opts.FlushInterval,
		MetricsCh:      opts.MetricsCh,
		MetricsCheck:   opts.MetricsCheck,
		NumWorkers:     opts.NumWorkers,
		RetryOnFailure: opts.RetryOnFailure,

		DocumentIDFunc: opts.DocumentIDFunc,
		FlushEndFunc:   opts.FlushEndFunc,
		FlushStartFunc: opts.FlushStartFunc,
		IndexNameFunc:  opts.IndexNameFunc,
		PauseDuration:  opts.PauseDuration,
		PauseFunc:      opts.PauseFunc,
		RefreshFunc:    opts.RefreshFunc,
		RoutingFunc:    opts.RoutingFunc,
	}

	if err := process(bO); err != nil {
		return nil, customerror.NewInvalidError("bulk options", customerror.WithError(err))
	}

	return bO, nil
}
