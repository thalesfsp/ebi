package ebi

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/ho"
	"github.com/thalesfsp/sypl"
	"github.com/thalesfsp/sypl/level"
)

//////
// Const, vars, and types.
//////

// Type of the entity.
const Type = "ebi"

// Operation represents the type of operation to be bulkly performed.
type Operation string

const (
	// Index is the default bulk operation.
	Index = "index"

	// Create documents, failing if it already exists.
	Create = "create"

	// Update is used to update documents. Control upsert behavious with the
	// option.
	Update = "update"

	// Delete is used to delete a documents.
	Delete = "delete"
)

// PauseFunc determines the conditions to pause the indexing process.
type PauseFunc func(metrics *Metrics) bool

// RefreshFunc determines the conditions to refresh the index. A good metric
// for that is the TranslogSize.
type RefreshFunc func(ctx context.Context, metrics *Metrics) bool

// EBI (Elasticsearch Bulk Indexer) is a generic struct that provides efficient
// bulk indexing capabilities for Elasticsearch. It contains an Elasticsearch client
// and logger for performing high-performance bulk operations with built-in monitoring
// and optimization features.
type EBI[T any] struct {
	logger sypl.ISypl

	client *elasticsearch.Client
}

// refreshIndex refreshes the specified Elasticsearch index, freeing up translog space.
// This operation forces a refresh to make recently indexed documents searchable
// and helps manage memory usage during bulk indexing operations.
func (ebi *EBI[T]) refreshIndex(ctx context.Context, indexName string) error {
	res, err := ebi.client.Indices.Refresh(
		ebi.client.Indices.Refresh.WithContext(ctx),
		ebi.client.Indices.Refresh.WithIndex(indexName),
	)
	if err != nil {
		return ErrorCatalog.
			MustGet(ErrFailedToRefreshIndex).
			NewFailedToError(customerror.WithError(err))
	}

	defer res.Body.Close()

	if res.IsError() {
		return ErrorCatalog.
			MustGet(ErrFailedToRefreshIndex).
			NewFailedToError(customerror.WithField("response", res.String()))
	}

	return nil
}

// Retrieve the node stats from Elasticsearch.
//
// NOTE: Common metrics: "jvm", "breaker", "indexing_pressure", "indices".
func (ebi *EBI[T]) retrieveNodeStats(ctx context.Context, metrics ...string) (*NodesStats, error) {
	//////
	// Nodes stats.
	//////

	opts := []func(*esapi.NodesStatsRequest){
		ebi.client.Nodes.Stats.WithContext(ctx),
	}

	if len(metrics) > 0 {
		opts = append(opts, ebi.client.Nodes.Stats.WithMetric(metrics...))
	}

	nodesStats, err := ebi.client.Nodes.Stats(opts...)
	if err != nil {
		return nil, ErrorCatalog.
			MustGet(ErrFailedToGetNodeStats).
			NewFailedToError(customerror.WithError(err))
	}

	defer nodesStats.Body.Close()

	var nodesStatsResp NodesStats
	if err := json.NewDecoder(nodesStats.Body).Decode(&nodesStatsResp); err != nil {
		return nil, ErrorCatalog.
			MustGet(ErrFailedToParseNodeStats).
			NewFailedToError(customerror.WithError(err))
	}

	return &nodesStatsResp, nil
}

// discoverWorkerNodes discovers and counts the number of data nodes in the ES cluster.
// Data nodes are responsible for holding data and performing data-related operations.
// This count is used to calculate the optimal number of workers for bulk indexing.
func (ebi *EBI[T]) discoverWorkerNodes(ctx context.Context) (int, error) {
	ns, err := ebi.retrieveNodeStats(ctx)
	if err != nil {
		return 0, ErrorCatalog.
			MustGet(ErrFailedToDiscoverNodes).
			NewFailedToError(customerror.WithError(err))
	}

	// Count nodes that have the data role: "data" is included in the roles list.
	dataNodes := 0

	for _, node := range ns.Nodes {
		if strings.Contains(strings.Join(node.Roles, ","), "data") {
			dataNodes++
		}
	}

	return dataNodes, nil
}

//////
// Exported functionalities.
//////

// BulkCreate indexes documents in Elasticsearch using the Bulk API.
//
// NOTE: Regarding BulkOptions, use NewBulkOptions to create a new instance.
//
//nolint:gocognit,maintidx,gocyclo
func (ebi *EBI[T]) BulkCreate(
	// Context to be used for the bulk indexing operation.
	ctx context.Context,

	// Documents to be indexed in Elasticsearch.
	docs []T,

	// Bulk options to configure the indexing process.
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
		return ErrorCatalog.
			MustGet(ErrFailedToCreateMetrics).
			NewFailedToError(customerror.WithError(err))
	}

	//////
	// Get initial node stats.
	//////

	if err := updateIndexMetrics(ctx, ebi, metrics); err != nil {
		return ErrorCatalog.
			MustGet(ErrFailedToUpdateMetrics).
			NewFailedToError(customerror.WithError(err))
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

	// Calculate the number of workers based on the JVM heap size.
	if opts.NumWorkers == nil {
		jvmHeapSizeGB := float64(metrics.ramPerNodeGB) / 2

		workersPerNode := int(jvmHeapSizeGB / 1)

		if workersPerNode < 1 {
			workersPerNode = 1
		}

		// Set number of workers.
		opts.NumWorkers = NumWorkersManual(metrics.numDataNodes * workersPerNode)
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
	// Number of worker nodes determination.
	//////

	ns, err := opts.NumWorkers()
	if err != nil {
		return ErrorCatalog.
			MustGet(ErrFailedToRetrieveNumWorkers).
			NewRequiredError()
	}

	//////
	// BI Setup.
	//////

	// Configure Bulk Indexer (BI).
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        ebi.client,
		Index:         opts.Index,
		NumWorkers:    ns,
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

				// Update ES metrics by updating `metrics`.
				if err := updateIndexMetrics(ctx, ebi, metrics); err != nil {
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
						// Pause if conditions are met.
						metrics.UpdateStatus(StatusPaused)
					} else {
						// Remove pause if conditions improve.
						metrics.UpdateStatus(StatusRunning)
					}
				}

				// Determine if we should refresh the index.
				if opts.RefreshFunc != nil && opts.RefreshFunc(ctx, metrics) {
					if err := ebi.refreshIndex(ctx, opts.Index); err != nil {
						// Send this async error to the error channel.
						asyncErrorHandler(
							ErrorCatalog.
								MustGet(ErrFailedToRefreshIndex).
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
			return ErrorCatalog.
				MustGet(ErrContextCancelled).
				NewFailedToError(customerror.WithError(err))
		}

		// Thread-safe check if the bulk indexer should be paused. If status is
		// "paused", then it pauses.
		if metrics.GetMetrics().Status == StatusPaused {
			time.Sleep(opts.PauseDuration)
		}

		// Properly marshal the document to JSON according to the operation type.
		var (
			data  []byte
			opErr error
		)

		switch opts.Operation {
		case "index", "create":
			// For index and create operations, we can use the document directly.
			data, opErr = json.Marshal(doc)
		case "update":
			// For update operations, wrap the document in a "doc" field.
			updateBody := map[string]any{
				"doc": doc,
			}

			// Add upsert behavior if enabled.
			if opts.DocAsUpsert {
				updateBody["doc_as_upsert"] = true
			}

			data, opErr = json.Marshal(updateBody)
		case "delete":
			// For delete operations, no body is needed.
			data = nil
		default:
			return ErrorCatalog.
				MustGet(ErrInvalidOperation).
				NewFailedToError(
					customerror.WithField("operation", opts.Operation),
					customerror.WithTag("ebi.BulkCreate"),
				)
		}

		if opErr != nil {
			// Update metrics.
			metrics.IncrementErrorCount()

			// Send this async error to the error channel.
			asyncErrorHandler(
				ErrorCatalog.
					MustGet(ErrFailedToConvertToJSON).
					NewFailedToError(
						customerror.WithError(opErr),
						customerror.WithTag("json.Marshal"),
					),
			)

			// We don't want to break the loop because of a single document.
			continue
		}

		bII := esutil.BulkIndexerItem{
			Action: opts.Operation, // e.g.: "index", "update", "delete", etc.
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

		// Should allow to specify document ID. If the document ID is not set,
		// it will be generated by Elasticsearch.
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

		// Set body only if we have data (not for delete operations)
		if data != nil {
			bII.Body = bytes.NewReader(data)
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

		// Update bytes processed only if we have data
		if data != nil {
			metrics.UpdateBytesProcessed(int64(len(data)))
		}
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

	ebi.logger.Debuglnf("Stats: %+v", stats)

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
	// Benchmark function that tests different parameter combinations.
	benchmarkFunc := func(params ...int) error {
		// Parameters to be optimized.
		opts.BatchSize = params[0]
		opts.NumWorkers = NumWorkersManual(params[1])

		// We don't care about metrics, just the error, if any
		// must be returned so the Gaussian Process can learn
		// and optimize the hyperparameters.
		if err := ebi.BulkCreate(
			ctx,
			docs,
			opts,
		); err != nil {
			return ErrorCatalog.
				MustGet(ErrHyperparameterOptimization).
				NewFailedToError(customerror.WithError(err))
		}

		return nil
	}

	// Run optimization with chosen configuration.
	optimalSize := ho.OptimizeHyperparameters(
		optimizationConfig,
		benchmarkFunc,
		parameterRange...,
	)

	return optimalSize, nil
}

//////
// Factory.
//////

// New creates and returns a new EBI (Elasticsearch Bulk Indexer) instance.
// It initializes the Elasticsearch client, tests the connection, and sets up
// logging.
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

		// Default level prints nothing.
		logger: sypl.NewDefault(Type, level.None),
	}, nil
}
