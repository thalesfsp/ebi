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

// PauseFunc determines the conditions to pause the indexing process. It
// receives a point-in-time snapshot of the metrics.
type PauseFunc func(metrics *Metrics) bool

// RefreshFunc determines the conditions to refresh the index. A good metric
// for that is the TranslogSize. It receives a point-in-time snapshot of the
// metrics.
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

	// An HTTP error payload (401/403/500...) would happily decode into a
	// zero NodesStats — 0 data nodes, bogus pressure, silently wrong
	// worker counts — so it must be rejected here.
	if nodesStats.IsError() {
		return nil, ErrorCatalog.
			MustGet(ErrFailedToGetNodeStats).
			NewFailedToError(customerror.WithField("response", nodesStats.String()))
	}

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
		// A null node entry decodes into a nil pointer.
		if node == nil {
			continue
		}

		if strings.Contains(strings.Join(node.Roles, ","), "data") {
			dataNodes++
		}
	}

	return dataNodes, nil
}

// bestEffortMetricsSend sends a point-in-time snapshot of metrics to ch
// without ever blocking: metrics are periodic telemetry, so when the
// buffer is full (or nobody is reading) dropping the snapshot is always
// preferable to stalling an indexing worker. Sending a snapshot (not the
// live *Metrics) keeps receivers race-free while writers keep mutating.
func bestEffortMetricsSend(ch chan<- *Metrics, m *Metrics) {
	if ch == nil {
		return
	}

	select {
	case ch <- m.GetMetrics():
	default:
	}
}

// asyncErrorSend sends err to errorCh without blocking if ctx is cancelled
// or errorCh is nil. Returns true if the error was delivered, false if
// ctx cancelled first or errorCh was nil.
//
// This is the deadlock-safe counterpart to a bare `errorCh <- err` send:
// callers can signal an error asynchronously without risking a hang when
// the reader goroutine has already exited (e.g. on context cancellation).
func asyncErrorSend(ctx context.Context, errorCh chan<- error, err error) bool {
	if errorCh == nil {
		return false
	}

	select {
	case errorCh <- err:
		return true
	case <-ctx.Done():
		return false
	}
}

//////
// Exported functionalities.
//////

// BulkCreate indexes documents in Elasticsearch using the Bulk API.
//
// Partial Updates:
//
// Updates are full. For partial updates, `DocAsUpsert` must be `true`.
// Take care on passing a typed struct, fields must have omitempty otherwise will
// empty fields!
//
// WARN: Regarding BulkOptions, use NewBulkOptions to create a new instance.
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

	// Options.
	if opts == nil {
		return ErrorCatalog.
			MustGet(ErrInvalidBulkOptions).
			NewRequiredError()
	}

	// Process a private shallow copy: process() applies `default` tags by
	// WRITING into zero fields, and BulkCreate treats the caller's opts as
	// read-only — it may be reused across calls or shared between
	// concurrent calls (see the derived-settings note below).
	optsCopy := *opts
	opts = &optsCopy

	if err := process(opts); err != nil {
		return ErrorCatalog.
			MustGet(ErrInvalidBulkOptions).
			NewInvalidError(customerror.WithError(err))
	}

	// NewBulkOptions rejects empty sample docs, but BulkCreate also
	// accepts caller-constructed struct literals — and the
	// `validate:"required"` tag passes for an empty-but-non-nil
	// json.RawMessage. Guard explicitly: SampleDoc's length is the divisor
	// of the batch-size calculation below (0 => divide-by-zero panic).
	if len(opts.SampleDoc) == 0 {
		return ErrorCatalog.
			MustGet(ErrSampleDocRequired).
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
	// Derived settings.
	//
	// NOTE: BulkCreate never writes back to opts: callers reuse the same
	// BulkOptions across calls (HyperparameterOptimization does exactly
	// that) and may share it between concurrent BulkCreate calls, so opts
	// is treated as read-only here — every derived value lives in a local.
	//////

	// Calculate the size of the sample document.
	docSize := len(opts.SampleDoc) // in bytes.

	// Determine the recommended batch size.
	targetBatchSizeBytes := 5 * 1024 * 1024 // Target batch size of 5MB.

	recommendedBatchSize := targetBatchSizeBytes / docSize

	if recommendedBatchSize < 1 {
		recommendedBatchSize = 1
	}

	// FlushBytes: the caller's value; else one caller-batch worth of bytes
	// — so a caller-set BatchSize drives the flush cadence; else the
	// recommendation (~ the 5MB target). The UNROUNDED recommendation is
	// used on purpose: rounding the min-1 clamp up to thousands would turn
	// an oversized sample doc (> target) into a multi-GB flush buffer.
	flushBytes := opts.FlushBytes
	if flushBytes <= 0 {
		if opts.BatchSize > 0 {
			flushBytes = opts.BatchSize * docSize
		} else {
			flushBytes = recommendedBatchSize * docSize
		}
	}

	// Calculate the number of workers based on the JVM heap size.
	numWorkersFn := opts.NumWorkers
	if numWorkersFn == nil {
		numDataNodes, ramPerNodeGB := metrics.getNodeInfo()

		jvmHeapSizeGB := float64(ramPerNodeGB) / 2

		workersPerNode := int(jvmHeapSizeGB / 1)

		if workersPerNode < 1 {
			workersPerNode = 1
		}

		numWorkersFn = NumWorkersManual(numDataNodes * workersPerNode)
	}

	//////
	// Final index name definition.
	//////

	// Modify the index name if a function is provided. The result stays
	// local: writing it back to opts.Index would compound the suffix on
	// every reuse of the same opts.
	indexName := opts.Index

	if opts.IndexNameFunc != nil {
		if name := opts.IndexNameFunc(indexName); name != "" {
			indexName = name
		}
	}

	if indexName == "" {
		return ErrorCatalog.
			MustGet(ErrIndexNameRequired).
			NewRequiredError()
	}

	//////
	// Number of worker nodes determination.
	//////

	ns, err := numWorkersFn()
	if err != nil {
		return ErrorCatalog.
			MustGet(ErrFailedToRetrieveNumWorkers).
			NewFailedToError(customerror.WithError(err))
	}

	//////
	// Internal helpers.
	//////

	// metricsCtx is an INTERNAL context that fires Done() exactly when
	// BulkCreate returns (via the deferred metricsCancel below). This
	// guarantees the metrics goroutine exits cleanly even when the
	// caller's `ctx` never fires Done() — for example when the caller
	// passes context.Background() or a context.WithoutCancel-derived
	// context (proj-ringboost-vendor cleanupCtx).
	//
	// Without this, the goroutine watches only the caller's ctx; once
	// BulkCreate's deferred ticker.Stop() runs, the goroutine is parked
	// in `select { case <-ctx.Done(): ...; case <-ticker.C: ... }` —
	// ticker.C does NOT close on Stop(), it just stops sending — so
	// the goroutine waits forever on a channel that will never deliver.
	// Each BulkCreate call leaks one such goroutine.
	//
	// 2026-05-01 production repro: rotation-stuck watchdog captured 3+
	// orphaned BulkCreate.func3 goroutines (98–609 minutes old, from
	// rotations 9–10 hours earlier). Regression test:
	// TestBulkCreate_NoMetricsGoroutineLeak_AfterReturn.
	metricsCtx, metricsCancel := context.WithCancel(ctx)
	defer metricsCancel()

	// Helper function to send async errors.
	//
	// Delegates to asyncErrorSend so the error send is ctx-aware — if the
	// reader goroutine has already exited (e.g. on context cancellation)
	// we won't deadlock on an unbuffered ErrorCh. Bound to metricsCtx, NOT
	// the caller's ctx: with a Background-like caller ctx and a dead
	// ErrorCh reader, a send guarded only by the caller's ctx would park
	// its goroutine forever even after BulkCreate returned.
	asyncErrorHandler := func(err error) {
		_ = asyncErrorSend(metricsCtx, opts.ErrorCh, err)
	}

	//////
	// BI Setup.
	//////

	// The ES bulk API accepts "true"/"false"/"wait_for" for refresh; the
	// exported RefreshPolicyImmediate constant is "immediate", so map it.
	refresh := opts.RefreshPolicy
	if refresh == RefreshPolicyImmediate {
		refresh = "true"
	}

	// Configure Bulk Indexer (BI).
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        ebi.client,
		Index:         indexName,
		NumWorkers:    ns,
		FlushBytes:    flushBytes,
		FlushInterval: opts.FlushInterval,
		Refresh:       refresh,
		OnFlushStart:  opts.FlushStartFunc,
		OnFlushEnd:    opts.FlushEndFunc,
		OnError: func(_ context.Context, err error) {
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

			// This callback runs on an esutil worker goroutine — it must
			// never block on a slow/absent metrics reader.
			bestEffortMetricsSend(opts.MetricsCh, metrics)
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

	// esutil's Close closes the internal queue channel, so a second Close
	// panics — closeBI makes it idempotent (touched only from this
	// goroutine, no lock needed). The main path closes explicitly BEFORE
	// reading Stats() so failures from the final flush are counted; the
	// deferred call below only covers early-return paths.
	biClosed := false

	closeBI := func() error {
		if biClosed {
			return nil
		}

		biClosed = true

		return bi.Close(ctx)
	}

	defer func() {
		// Cancelling the metrics context BEFORE closing is load-bearing
		// on error paths: Close blocks until the workers flush, and the
		// worker callbacks' error sends are guarded by metricsCtx — with
		// it cancelled they can never hang this return.
		metricsCancel()

		// Best-effort close: the main path already closed and consumed
		// the error; here an error is already being returned.
		_ = closeBI()
	}()

	//////
	// Metrics monitoring goroutine.
	//////

	// Start metrics monitoring goroutine. It exits via metricsCtx (see
	// its comment above) when BulkCreate returns.
	metricsTicker := time.NewTicker(opts.MetricsCheck)

	defer metricsTicker.Stop()

	go func() {
		for {
			select {
			case <-metricsCtx.Done():
				return
			case <-metricsTicker.C:
				// Update ES metrics by updating `metrics`.
				//
				// NOTE: The goroutine's ES calls run under metricsCtx (not
				// the caller's ctx) so metricsCancel can interrupt an
				// in-flight request once BulkCreate returns. Errors caused
				// by that wind-down cancellation are suppressed — they are
				// not indexing failures.
				if err := updateIndexMetrics(metricsCtx, ebi, metrics); err != nil &&
					metricsCtx.Err() == nil {
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

				// Point-in-time snapshot for the user callbacks: handing
				// them the live *Metrics would let their field reads race
				// with the esutil worker callbacks mutating it.
				tickMetrics := metrics.GetMetrics()

				// Determine if we should pause the bulk indexer.
				if opts.PauseFunc != nil {
					if opts.PauseFunc(tickMetrics) {
						// Pause if conditions are met.
						metrics.UpdateStatus(StatusPaused)
					} else {
						// Remove pause if conditions improve.
						metrics.UpdateStatus(StatusRunning)
					}
				}

				// Determine if we should refresh the index.
				if opts.RefreshFunc != nil && opts.RefreshFunc(metricsCtx, tickMetrics) {
					if err := ebi.refreshIndex(metricsCtx, indexName); err != nil &&
						metricsCtx.Err() == nil {
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

				// Per-tick metrics delivery. Best-effort and inline —
				// deferring these sends would accumulate one per tick and
				// fire them all at goroutine exit, parking the goroutine
				// forever when nobody reads the channel.
				bestEffortMetricsSend(opts.MetricsCh, metrics)
			}
		}
	}()

	// Bulk insertion.
	//
	// NOTE: This is a parallel, asynchronous, efficient indexing process.
	for _, doc := range docs {
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
		case Index, Create:
			// For index and create operations, we can use the document directly.
			data, opErr = json.Marshal(doc)
		case Update:
			// For update operations, wrap the document in a "doc" field.
			updateBody := map[string]any{
				"doc": doc,
			}

			// Sets doc_as_upsert behavior only if enabled.
			// For true partial updates, omit doc_as_upsert entirely when false.
			if opts.DocAsUpsert {
				updateBody["doc_as_upsert"] = true
			}

			data, opErr = json.Marshal(updateBody)
		case Delete:
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
			Index:  indexName,

			// Document successfully indexed.
			OnSuccess: func(_ context.Context, _ esutil.BulkIndexerItem, _ esutil.BulkIndexerResponseItem) {
				// Update metrics.
				metrics.IncreaseDocsSucceeded()

				// This callback runs on an esutil worker goroutine — it
				// must never block on a slow/absent metrics reader.
				bestEffortMetricsSend(opts.MetricsCh, metrics)
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

				// This callback runs on an esutil worker goroutine — it
				// must never block on a slow/absent metrics reader.
				bestEffortMetricsSend(opts.MetricsCh, metrics)
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
			// A cancellation racing the Add surfaces here as esutil's
			// wrapped ctx error; report it uniformly as the cancellation
			// it is, matching the top-of-loop check.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ErrorCatalog.
					MustGet(ErrContextCancelled).
					NewFailedToError(customerror.WithError(ctxErr))
			}

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

		// Per-document metrics delivery. Best-effort and inline —
		// deferring these sends would accumulate one per document and
		// fire them all (blocking) at return time.
		bestEffortMetricsSend(opts.MetricsCh, metrics)
	}

	// Flush the remaining buffered documents BEFORE reading the stats:
	// with the default FlushBytes and a small doc set the ONLY flush
	// happens inside Close — reading Stats() first would miss every
	// failure from that final flush and report success.
	if err := closeBI(); err != nil {
		return ErrorCatalog.
			MustGet(ErrFailedToBulkIndexDocuments).
			NewFailedToError(
				customerror.WithError(err),
				customerror.WithTag("bi.Close"),
			)
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

	// Ensures metrics channel is updated - one last time (best-effort).
	bestEffortMetricsSend(opts.MetricsCh, metrics)

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

	// A transport-level success can still be an HTTP error (401/403/503):
	// without this check an unreachable-in-practice cluster yields a
	// "working" client.
	if res.IsError() {
		return nil, ErrorCatalog.
			MustGet(ErrFailedToPing).
			NewFailedToError(customerror.WithField("response", res.String()))
	}

	return &EBI[T]{
		client: client,

		// Default level prints nothing.
		logger: sypl.NewDefault(Type, level.None),
	}, nil
}
