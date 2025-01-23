package ebi

import (
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

// BulkOptions defines the options for bulk indexing.
//
// NOTE: Use NewBulkOptions() to create a new BulkOptions struct!
//
//nolint:lll
type BulkOptions[T any] struct {
	// These are options that can be used in the hyperparameter tuning.
	BatchSize  int `json:"batchSize"`
	NumWorkers int `json:"numWorkers"`

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
		Index:         indexName,
		SampleDoc:     sampleDoc,
		RefreshPolicy: opts.RefreshPolicy,

		BatchSize:      0,
		NumWorkers:     0,
		FlushBytes:     0,
		FlushInterval:  30 * time.Second,
		MetricsCheck:   5 * time.Second,
		MetricsCh:      opts.MetricsCh,
		ErrorCh:        opts.ErrorCh,
		RetryOnFailure: 3,

		DocumentIDFunc: opts.DocumentIDFunc,
		IndexNameFunc:  opts.IndexNameFunc,
		RoutingFunc:    opts.RoutingFunc,
		PauseFunc:      opts.PauseFunc,
		PauseDuration:  5 * time.Second,
		RefreshFunc:    opts.RefreshFunc,
	}

	if err := process(bO); err != nil {
		return nil, customerror.NewInvalidError("bulk options", customerror.WithError(err))
	}

	return bO, nil
}
