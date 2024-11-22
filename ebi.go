package ebi

import (
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/ho"
)

//////
// Const, vars, and types.
//////

// ebi is a struct that contains the Elasticsearch client.
type ebi[T any] struct {
	client *elasticsearch.Client
}

//////
// Exported functionalities.
//////

// BulkCreate indexes documents in Elasticsearch using the Bulk API.
//
//nolint:gocognit,maintidx
func (eSI *ebi[T]) BulkCreate(
	ctx context.Context,
	docs []T,
	opts BulkOptions[T],

	// For async metrics, optional.
	globalMetricsCh chan<- *GlobalMetrics,

	// For async errors, optional.
	errorCh chan<- error,
) error {
	//////
	// Docs validation.
	//////

	if len(docs) == 0 {
		return ErrorCatalog.
			MustGet(ErrDocumentsRequired).
			NewRequiredError()
	}

	//////
	// Internal helper.
	//////

	// Helper function to send async errors.
	asyncErrorHandler := func(err error) {
		if errorCh != nil {
			errorCh <- err
		}
	}

	//////
	// Metrics initialization.
	//////

	// Initialize bulk metrics with explicit reset
	bulkMetrics := &BulkMetrics{
		DocsProcessed:  0,
		DocsFailed:     0,
		DocsSucceeded:  0,
		BytesProcessed: 0,
		ErrorCount:     0,
		FailedItems:    make([]FailedItem, 0),
		LastError:      nil,
	}

	// Initialize index metrics.
	indexMetrics := &IndexMetrics{
		CircuitBreakers:  make(map[string]float64),
		IndexingPressure: 0,
		JVMHeapUsage:     0,
		SegmentsCount:    0,
		SegmentsMemory:   0,
		TranslogSize:     0,
		UnassignedShards: 0,
	}

	// Initialize global metrics.
	globalMetrics := &GlobalMetrics{
		BulkMetrics:  bulkMetrics,
		IndexMetrics: indexMetrics,
	}

	//////
	// Final index name definition.
	//////

	// Modify the index name if a function is provided.
	if opts.IndexNameFunc != nil {
		opts.Index = opts.IndexNameFunc(opts.Index)
	}

	//////
	// BI Setup.
	//////

	// Configure Bulk Indexer (BI).
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        eSI.client,
		Index:         opts.Index,
		NumWorkers:    opts.NumWorkers,
		FlushBytes:    opts.FlushBytes,
		FlushInterval: opts.FlushInterval,
		OnError: func(_ context.Context, err error) {
			// Update metrics.
			bulkMetrics.LastError = err

			// Send this async error to the error channel.
			asyncErrorHandler(
				ErrorCatalog.
					MustGet(ErrIndexerError).
					NewFailedToError(
						customerror.WithError(err),
						customerror.WithTag("esutil.NewBulkIndexer"),
					),
			)
		},
	})
	if err != nil {
		return ErrorCatalog.
			MustGet(ErrFailedToCreateBulkIndexer).
			NewFailedToError(
				customerror.WithError(err),
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
				if err := updateIndexMetrics(ctx, eSI.client, indexMetrics); err != nil {
					// Send this async error to the error channel.
					asyncErrorHandler(
						ErrorCatalog.
							MustGet(ErrFailedToUpdateMetrics).
							NewFailedToError(
								customerror.WithError(err),
								customerror.WithTag("updateIndexMetrics"),
							),
					)

					continue
				}

				// Send metrics to the channel.
				if globalMetricsCh != nil {
					globalMetricsCh <- globalMetrics
				}

				// For everytime metrics update, determine if should pause.
				if shouldTemporaryPause(*indexMetrics, opts) {
					// TODO: Make it configurable.
					time.Sleep(5 * time.Second)
				}
			}
		}
	}()

	// Bulk insertion.
	//
	// NOTE: This is a parallel, asynchronous, efficient indexing process.
	for _, doc := range docs {
		// Breaks the loop if the context errored by any reason.
		if err := ctx.Err(); err != nil {
			return err
		}

		// Convert document to JSON.
		data, err := json.Marshal(doc)
		if err != nil {
			// Update metrics.
			bulkMetrics.LastError = err

			atomic.AddInt64(&bulkMetrics.DocsFailed, 1)

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
				atomic.AddInt64(&bulkMetrics.DocsSucceeded, 1)
			},

			// Document failed to index.
			OnFailure: func(_ context.Context, _ esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				// Send this async error to the error channel.
				asyncErrorHandler(
					ErrorCatalog.
						MustGet(ErrFailedToIndexDocument).
						NewFailedToError(
							customerror.WithError(err),
							customerror.WithTag("bi.Add"),
							customerror.WithField("docID", res.DocumentID),
						),
				)

				// Update metrics.
				atomic.AddInt64(&bulkMetrics.DocsFailed, 1)

				bulkMetrics.LastError = err

				bulkMetrics.FailedItems = append(bulkMetrics.FailedItems, FailedItem{
					ID:     res.DocumentID,
					Reason: res.Error.Reason,
					Status: res.Status,
				})
			},
		}

		// Should allow to specify document ID.
		if opts.DocumentIDFunc != nil {
			bII.DocumentID = opts.DocumentIDFunc(doc)
		}

		// Should allow to specify routing.
		if opts.RoutingFunc != nil {
			bII.Routing = opts.RoutingFunc(doc)
		}

		if err := bi.Add(ctx, bII); err != nil {
			return ErrorCatalog.
				MustGet(ErrFailedToAddDocumentToBulkIndexer).
				NewFailedToError(
					customerror.WithError(err),
					customerror.WithTag("bi.Add"),
				)
		}

		// Update metrics.
		atomic.AddInt64(&bulkMetrics.DocsProcessed, 1)

		atomic.AddInt64(&bulkMetrics.BytesProcessed, int64(len(data)))
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

	// Optionally refresh index.
	if indexMetrics.TranslogSize > opts.MaxTranslogSize {
		if err := refreshIndex(ctx, eSI.client, opts.Index); err != nil {
			return ErrorCatalog.
				MustGet(ErrFailedToRefreshIndex).
				NewFailedToError(
					customerror.WithError(err),
					customerror.WithTag("refreshIndex"),
				)
		}
	}

	return nil
}

// HyperparameterOptimization runs a hyperparameter optimization
// using the Gaussian Process and Upper Confidence Bound (UCB)
// as acquisition function against batch size and number of workers.
func (eSI *ebi[T]) HyperparameterOptimization(
	// Context to be used in the optimization.
	ctx context.Context,

	// Bulk options to be optimized.
	opts BulkOptions[T],

	// Documents to be used in the optimization.
	docs []T,

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
		if err := eSI.BulkCreate(
			ctx,
			docs,
			opts,
			nil,
			nil,
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
) (*ebi[T], error) {
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

	return &ebi[T]{
		client: client,
	}, nil
}
