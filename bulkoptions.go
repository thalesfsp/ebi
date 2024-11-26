package ebi

import (
	"encoding/json"
	"time"

	"github.com/thalesfsp/customerror"
)

//////
// Const, vars, and types.
//////

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
// Factory.
//////

// NewBulkOptions calculates the bulk indexing options based on the
// sample document size and cluster configuration.
func NewBulkOptions[T any](
	// Index name.
	indexName string,

	// Sample document.
	sampleDoc json.RawMessage,

	// Refresh policy.
	refreshPolicy RefreshPolicy,

	// Optional functions.
	indexNameFunc func(indexName string) string,
	documentIDFunc func(doc T) string,
	routingFunc func(doc T) string,
	pauseFunc PauseFunc,
	refreshFunc RefreshFunc,
) (*BulkOptions[T], error) {
	bO := &BulkOptions[T]{
		Index:         indexName,
		SampleDoc:     sampleDoc,
		RefreshPolicy: refreshPolicy,

		BatchSize:      0,
		NumWorkers:     0,
		FlushBytes:     0,
		FlushInterval:  30 * time.Second,
		MetricsCheck:   5 * time.Second,
		RetryOnFailure: 3,

		DocumentIDFunc: documentIDFunc,
		IndexNameFunc:  indexNameFunc,
		RoutingFunc:    routingFunc,
		PauseFunc:      pauseFunc,
		PauseDuration:  5 * time.Second,
		RefreshFunc:    refreshFunc,
	}

	if err := process(bO); err != nil {
		return nil, customerror.NewInvalidError("bulk options", customerror.WithError(err))
	}

	return bO, nil
}
