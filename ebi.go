package ebi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/ho"
)

//////
// Const, vars, and types.
//////

// PauseFunc determines the conditions to pause the indexing process.
type PauseFunc func(metrics *Metrics) bool

// RefreshFunc determines the conditions to refresh the index. A good metrics
// for that is the TranslogSize.
type RefreshFunc func(ctx context.Context, metrics *Metrics) bool

// EBI is a struct that contains the Elasticsearch client.
type EBI[T any] struct {
	client *elasticsearch.Client
}

// Refresh the index freeing up translog space.
func (ebi *EBI[T]) refreshIndex(ctx context.Context, indexName string) error {
	res, err := ebi.client.Indices.Refresh(
		ebi.client.Indices.Refresh.WithContext(ctx),
		ebi.client.Indices.Refresh.WithIndex(indexName),
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
// Exported functionalities.
//////

// BulkCreate indexes documents in Elasticsearch using the Bulk API.
//
// NOTE: Regarding BulkOptions, use NewBulkOptions to create a new instance.
//
//nolint:gocognit,maintidx,nestif,gocyclo
func (ebi *EBI[T]) BulkCreate(
	// Context to be used in the optimization.
	ctx context.Context,

	// Documents to be used in the optimization.
	docs []T,

	// Bulk options to be optimized.
	opts *BulkOptions[T],
) error {
	//////
	// Validation.
	//////

	// Docs.
	if len(docs) == 0 {
		return ErrorCatalog.
			MustGet(ErrDocumentsRequired).
			NewRequiredError()
	}

	// Process.
	if err := process(opts); err != nil {
		return ErrorCatalog.
			MustGet(ErrInvalidBulkOptions).
			NewRequiredError()
	}

	//////
	// Metrics initialization.
	//////

	// Initialize metrics.
	metrics, err := NewMetrics()
	if err != nil {
		return err
	}

	//////
	// Get initial node stats.
	//////

	if err := updateIndexMetrics(ctx, ebi.client, metrics); err != nil {
		return err
	}

	//////
	// Calculate recommended number of workers.
	//////

	// Calculate the size of the sample document.
	docSize := len(opts.SampleDoc) // in bytes.

	// Determine the recommended batch size.
	targetBatchSizeBytes := 5 * 1024 * 1024 // Target batch size of 5MB.

	recommendedBatchSize := targetBatchSizeBytes / docSize

	if recommendedBatchSize < 1 {
		recommendedBatchSize = 1
	}

	// Set batch size.
	if opts.BatchSize == 0 {
		opts.BatchSize = roundToNearestThousandDivisibleBy2(recommendedBatchSize)
	}

	if opts.NumWorkers == 0 {
		jvmHeapSizeGB := float64(metrics.ramPerNodeGB) / 2

		workersPerNode := int(jvmHeapSizeGB / 1)

		if workersPerNode < 1 {
			workersPerNode = 1
		}

		// Set number of workers.
		opts.NumWorkers = metrics.numDataNodes * workersPerNode
	}

	// Set FlushBytes size.
	opts.FlushBytes = recommendedBatchSize * docSize

	//////
	// Internal helper.
	//////

	// Helper function to send async errors.
	asyncErrorHandler := func(err error) {
		if opts.ErrorCh != nil {
			opts.ErrorCh <- err
		}
	}

	//////
	// Final index name definition.
	//////

	// Modify the index name if a function is provided.
	if opts.IndexNameFunc != nil {
		indexName := opts.IndexNameFunc(opts.Index)

		if indexName != "" {
			opts.Index = indexName
		}
	}

	if opts.Index == "" {
		return ErrorCatalog.
			MustGet(ErrIndexNameRequired).
			NewRequiredError()
	}

	//////
	// BI Setup.
	//////

	// Configure Bulk Indexer (BI).
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        ebi.client,
		Index:         opts.Index,
		NumWorkers:    opts.NumWorkers,
		FlushBytes:    opts.FlushBytes,
		FlushInterval: opts.FlushInterval,
		OnError: func(_ context.Context, err error) {
			defer func() {
				// Ensures metrics channel is updated.
				if opts.MetricsCh != nil {
					opts.MetricsCh <- metrics
				}
			}()

			// Update metrics.
			metrics.IncrementErrorCount()

			// Send this async error to the error channel.
			asyncErrorHandler(
				ErrorCatalog.
					MustGet(ErrIndexerError).
					NewFailedToError(
						customerror.WithError(err),
						customerror.WithTag("esutil.NewBulkIndexer.OnError"),
					),
			)
		},
	})
	if err != nil {
		return ErrorCatalog.
			MustGet(ErrFailedToCreateBulkIndexer).
			NewFailedToError(
				customerror.WithError(err),
				customerror.WithTag("esutil.NewBulkIndexer"),
			)
	}

	defer bi.Close(ctx)

	//////
	// Metrics monitoring goroutine.
	//////

	// Start metrics monitoring goroutine.
	metricsTicker := time.NewTicker(opts.MetricsCheck)

	defer metricsTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-metricsTicker.C:
				defer func() {
					// Ensures metrics channel is updated.
					if opts.MetricsCh != nil {
						opts.MetricsCh <- metrics
					}
				}()

				// Bring ES metrics updating `metrics`.
				if err := updateIndexMetrics(ctx, ebi.client, metrics); err != nil {
					// Send this async error to the error channel.
					asyncErrorHandler(
						ErrorCatalog.
							MustGet(ErrFailedToUpdateMetrics).
							NewFailedToError(
								customerror.WithError(err),
								customerror.WithTag("metricsloop.updateIndexMetrics"),
							),
					)
				}

				// Determine if we should pause the bulk indexer.
				if opts.PauseFunc != nil {
					if opts.PauseFunc(metrics) {
						// Pause the if met conditions.
						metrics.UpdateStatus(StatusPaused)
					} else {
						// Remove pause if conditions improves.
						metrics.UpdateStatus(StatusRunning)
					}
				}

				// Determine if we should refresh the index.
				if opts.RefreshFunc != nil && opts.RefreshFunc(ctx, metrics) {
					if err := ebi.refreshIndex(ctx, opts.Index); err != nil {
						// Send this async error to the error channel.
						asyncErrorHandler(
							ErrorCatalog.
								MustGet(ErrFailedToUpdateMetrics).
								NewFailedToError(
									customerror.WithError(err),
									customerror.WithTag("metricsloop.refreshIndex"),
								),
						)
					}
				}
			}
		}
	}()

	// Bulk insertion.
	//
	// NOTE: This is a parallel, asynchronous, efficient indexing process.
	for _, doc := range docs {
		defer func() {
			// Ensures metrics channel is updated.
			if opts.MetricsCh != nil {
				opts.MetricsCh <- metrics
			}
		}()

		// Breaks the loop if the context errored by any reason.
		if err := ctx.Err(); err != nil {
			return err
		}

		// Thread-safe check if the bulk indexer should be pause. If status is
		// to "paused", then it pauses.
		if metrics.GetMetrics().Status == StatusPaused {
			time.Sleep(opts.PauseDuration)
		}

		// Convert document to JSON.
		data, err := json.Marshal(doc)
		if err != nil {
			// Update metrics.
			metrics.IncrementErrorCount()

			// Send this async error to the error channel.
			asyncErrorHandler(
				ErrorCatalog.
					MustGet(ErrFailedToConvertToJSON).
					NewFailedToError(
						customerror.WithError(err),
						customerror.WithTag("json.Marshal"),
					),
			)

			// We don't want to break the loop because of a single document.
			continue
		}

		bII := esutil.BulkIndexerItem{
			Action: "create",
			Body:   bytes.NewReader(data),
			Index:  opts.Index,

			// Document successfully indexed.
			OnSuccess: func(_ context.Context, _ esutil.BulkIndexerItem, _ esutil.BulkIndexerResponseItem) {
				// Update metrics.
				metrics.IncreaseDocsSucceeded()

				// Ensures metrics channel is updated.
				if opts.MetricsCh != nil {
					opts.MetricsCh <- metrics
				}
			},

			// Document failed to index.
			OnFailure: func(_ context.Context, bII esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				errorOpts := []customerror.Option{
					customerror.WithField("res.Error", res.Error),
					customerror.WithField("bII.DocumentID", bII.DocumentID),
					customerror.WithField("res.DocID", res.DocumentID),
					customerror.WithTag("bi.Add"),
				}

				if err != nil {
					errorOpts = append(errorOpts, customerror.WithError(err))
					errorOpts = append(errorOpts, customerror.WithField("error", err.Error()))
				}

				// Send this async error to the error channel.
				asyncErrorHandler(
					ErrorCatalog.
						MustGet(ErrFailedToIndexDocument).
						NewFailedToError(
							errorOpts...,
						),
				)

				// Update metrics.
				metrics.IncreaseDocsFailed()

				metrics.IncrementErrorCount()

				// Ensures metrics channel is updated.
				if opts.MetricsCh != nil {
					opts.MetricsCh <- metrics
				}
			},
		}

		// Should allow to specify document ID.
		if opts.DocumentIDFunc != nil {
			id := opts.DocumentIDFunc(doc)

			if id != "" {
				bII.DocumentID = id
			}
		}

		// Should allow to specify routing.
		if opts.RoutingFunc != nil {
			routing := opts.RoutingFunc(doc)

			if routing != "" {
				bII.Routing = routing
			}
		}

		// Actually add the document to the bulk indexer.
		if err := bi.Add(ctx, bII); err != nil {
			return ErrorCatalog.
				MustGet(ErrFailedToAddDocumentToBulkIndexer).
				NewFailedToError(
					customerror.WithError(err),
					customerror.WithTag("bi.Add"),
				)
		}

		// Update metrics.
		metrics.IncreaseDocsProcessed()

		metrics.UpdateBytesProcessed(int64(len(data)))
	}

	// Final statistics.
	stats := bi.Stats()

	// Check if any document failed to index.
	if stats.NumFailed > 0 {
		return ErrorCatalog.
			MustGet(ErrFailedToBulkIndexDocuments).
			NewFailedToError(
				customerror.WithField("failedDocs", stats.NumFailed),
				customerror.WithTag("bulkCreate"),
			)
	}

	// Ensures metrics channel is updated - one last time.
	if opts.MetricsCh != nil {
		opts.MetricsCh <- metrics
	}

	return nil
}

// HyperparameterOptimization runs a hyperparameter optimization
// using the Gaussian Process and Upper Confidence Bound (UCB)
// as acquisition function against batch size and number of workers.
//
// NOTE: Regarding BulkOptions, use NewBulkOptions to create a new instance.
func (ebi *EBI[T]) HyperparameterOptimization(
	// Context to be used in the optimization.
	ctx context.Context,

	// Documents to be used in the optimization.
	docs []T,

	// Bulk options to be optimized.
	opts *BulkOptions[T],

	// Optimization configuration.
	optimizationConfig ho.OptimizationConfig,

	// Parameter range to be optimized.
	parameterRange []ho.ParameterRange[int],
) ([]int, error) {
	// Your benchmark function
	benchmarkFunc := func(params ...int) error {
		// Parameters to be optimized.
		opts.BatchSize = params[0]
		opts.NumWorkers = params[1]

		// We don't care about metrics, just the error, if any
		// must be returned so the Gaussian Process can learn
		// and optimize the hyperparameters.
		if err := ebi.BulkCreate(
			ctx,
			docs,
			opts,
		); err != nil {
			return err
		}

		return nil
	}

	// Run optimization with chosen configuration.
	optimalSize := ho.OptimizeHyperparameters[int](
		optimizationConfig,
		benchmarkFunc,
		parameterRange...,
	)

	return optimalSize, nil
}

//////
// Factory.
//////

// New returns a new ebi.
func New[T any](
	ctx context.Context,
	esConfig elasticsearch.Config,
) (*EBI[T], error) {
	// Create the client.
	client, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		return nil, ErrorCatalog.
			MustGet(ErrFailedToCreateClient).
			NewFailedToError(customerror.WithError(err))
	}

	// Test the connection.
	res, err := client.Ping(client.Ping.WithContext(ctx))
	if err != nil {
		return nil, ErrorCatalog.
			MustGet(ErrFailedToPing).
			NewFailedToError(customerror.WithError(err))
	}

	defer res.Body.Close()

	return &EBI[T]{
		client: client,
	}, nil
}
