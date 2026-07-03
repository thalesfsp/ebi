package ebi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file is the regression suite for the 2026-07 audit findings (B1-B14):
// channel deadlocks, metrics snapshot races, opts mutation/reuse hazards,
// stats-read-before-final-flush, dead RefreshPolicy/FlushStartFunc/
// FlushEndFunc/DocAsUpsert options, and unchecked HTTP error responses.
//
// Everything runs against captureES, a fake Elasticsearch server that
// CAPTURES requests (paths, query params, parsed NDJSON bulk bodies) and
// returns configurable bulk responses — extending the minimal fakeESServer
// from ebi_leak_test.go.

// auditTimeout bounds every BulkCreate call in this file: the pre-fix bugs
// under test are deadlocks, so an unguarded call would hang the whole suite
// instead of failing.
const auditTimeout = 10 * time.Second

//////
// Fake ES server with request capture.
//////

// bulkAction is one parsed action from an NDJSON /_bulk request body,
// paired with its source document (nil for delete operations).
type bulkAction struct {
	Op     string          // "index", "create", "update" or "delete".
	Index  string          // _index from the action metadata line.
	DocID  string          // _id from the action metadata line.
	Source json.RawMessage // The line following the action; nil for delete.
}

// capturedBulk is one captured request to the /_bulk endpoint.
type capturedBulk struct {
	Path    string
	Query   url.Values
	Actions []bulkAction
}

// captureESConfig configures captureES behavior. Set at construction only —
// the handler runs on server goroutines, so mutating after first use would
// race.
type captureESConfig struct {
	// pingStatus, when non-zero, forces this HTTP status on HEAD / (the
	// Ping request issued by New).
	pingStatus int

	// nodesStatsStatus/nodesStatsBody, when status is non-zero, force
	// every /_nodes/stats response.
	nodesStatsStatus int
	nodesStatsBody   string

	// refreshStatus, when non-zero, forces this status on /_refresh.
	refreshStatus int

	// failBulkItems makes /_bulk respond per-item status 400 errors with
	// "errors":true — every document fails.
	failBulkItems bool
}

// captureES is the richer fake ES server used by the audit regression
// suite.
type captureES struct {
	srv *httptest.Server
	cfg captureESConfig

	mu   chan struct{} // 1-token semaphore; keeps the struct testify-free.
	bulk []capturedBulk
}

func (f *captureES) lock()   { f.mu <- struct{}{} }
func (f *captureES) unlock() { <-f.mu }

// bulkRequests returns a snapshot of the captured /_bulk requests.
func (f *captureES) bulkRequests() []capturedBulk {
	f.lock()
	defer f.unlock()

	out := make([]capturedBulk, len(f.bulk))
	copy(out, f.bulk)

	return out
}

// allActions flattens the actions of every captured /_bulk request.
func (f *captureES) allActions() []bulkAction {
	var acc []bulkAction
	for _, b := range f.bulkRequests() {
		acc = append(acc, b.Actions...)
	}

	return acc
}

// parseBulkNDJSON parses an NDJSON bulk body into actions. Returns nil on
// malformed input (the caller responds 400).
func parseBulkNDJSON(body []byte) []bulkAction {
	var actions []bulkAction

	lines := bytes.Split(body, []byte("\n"))
	for i := 0; i < len(lines); i++ {
		line := bytes.TrimSpace(lines[i])
		if len(line) == 0 {
			continue
		}

		var meta map[string]struct {
			Index string `json:"_index"`
			ID    string `json:"_id"`
		}
		if err := json.Unmarshal(line, &meta); err != nil || len(meta) != 1 {
			return nil
		}

		var act bulkAction
		for op, m := range meta {
			act = bulkAction{Op: op, Index: m.Index, DocID: m.ID}
		}

		// Every operation except delete is followed by a source line.
		if act.Op != "delete" {
			i++
			for i < len(lines) && len(bytes.TrimSpace(lines[i])) == 0 {
				i++
			}
			if i >= len(lines) {
				return nil
			}
			act.Source = json.RawMessage(bytes.TrimSpace(lines[i]))
		}

		actions = append(actions, act)
	}

	return actions
}

// newCaptureES starts the capture server. Response shape matches what the
// v8 client + esutil expect (X-Elastic-Product header, per-item bulk
// responses positionally aligned with the request actions).
func newCaptureES(t *testing.T, cfg captureESConfig) *captureES {
	t.Helper()

	f := &captureES{cfg: cfg, mu: make(chan struct{}, 1)}

	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")

		path := r.URL.Path
		switch {
		case path == "/" || path == "":
			if r.Method == http.MethodHead {
				if cfg.pingStatus != 0 {
					w.WriteHeader(cfg.pingStatus)
				}
				return
			}
			_, _ = w.Write([]byte(`{
				"name":"node1","cluster_name":"test-cluster","cluster_uuid":"test-uuid",
				"version":{"number":"8.11.0","build_flavor":"default","build_type":"docker",
					"build_hash":"abc","build_date":"2024-01-01","build_snapshot":false,
					"lucene_version":"9.0.0","minimum_wire_compatibility_version":"7.17.0",
					"minimum_index_compatibility_version":"7.0.0"},
				"tagline":"You Know, for Search"
			}`))
		case strings.Contains(path, "_nodes/stats"):
			if cfg.nodesStatsStatus != 0 {
				w.WriteHeader(cfg.nodesStatsStatus)
				_, _ = w.Write([]byte(cfg.nodesStatsBody))
				return
			}
			_, _ = w.Write([]byte(`{
				"_nodes":{"total":1,"successful":1,"failed":0},
				"cluster_name":"test-cluster",
				"nodes":{}
			}`))
		case strings.Contains(path, "_bulk"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			actions := parseBulkNDJSON(body)
			if actions == nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"malformed NDJSON"}`))
				return
			}

			f.lock()
			f.bulk = append(f.bulk, capturedBulk{
				Path:    path,
				Query:   r.URL.Query(),
				Actions: actions,
			})
			f.unlock()

			// One response item per action, positionally aligned —
			// esutil indexes response items against its in-flight items.
			items := make([]map[string]any, 0, len(actions))
			for _, a := range actions {
				if cfg.failBulkItems {
					items = append(items, map[string]any{a.Op: map[string]any{
						"_index": a.Index, "_id": a.DocID, "status": 400,
						"error": map[string]any{
							"type":   "mapper_parsing_exception",
							"reason": "forced item failure",
						},
					}})
					continue
				}

				items = append(items, map[string]any{a.Op: map[string]any{
					"_index": a.Index, "_id": a.DocID, "status": 200,
					"result": map[string]string{
						"index": "created", "create": "created",
						"update": "updated", "delete": "deleted",
					}[a.Op],
				}})
			}

			_ = json.NewEncoder(w).Encode(map[string]any{
				"took": 1, "errors": cfg.failBulkItems, "items": items,
			})
		case strings.HasSuffix(path, "/_refresh"):
			if cfg.refreshStatus != 0 {
				w.WriteHeader(cfg.refreshStatus)
				_, _ = w.Write([]byte(`{"error":{"type":"forced","reason":"forced refresh failure"},"status":500}`))
				return
			}
			_, _ = w.Write([]byte(`{"_shards":{"total":1,"successful":1,"failed":0}}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))

	t.Cleanup(f.srv.Close)

	return f
}

// runBulkCreateGuarded runs BulkCreate under the audit timeout so a
// regressed deadlock fails the test instead of hanging the suite.
func runBulkCreateGuarded(
	t *testing.T,
	ctx context.Context,
	e *EBI[*TestModel],
	docs []*TestModel,
	opts *BulkOptions[*TestModel],
) error {
	t.Helper()

	done := make(chan error, 1)

	go func() {
		done <- e.BulkCreate(ctx, docs, opts)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(auditTimeout):
		t.Fatal("deadlock: BulkCreate did not return within the audit timeout")
		return nil
	}
}

//////
// Unit: option wiring (B9).
//////

// TestNewBulkOptions_DocAsUpsert pins B9: WithDocAsUpsert(false) used to be
// impossible — the value was dropped by NewBulkOptions and then flipped
// back to true by the `default:"true"` tag inside process().
func TestNewBulkOptions_DocAsUpsert(t *testing.T) {
	t.Parallel()

	t.Run("default is true", func(t *testing.T) {
		t.Parallel()

		opts, err := NewBulkOptions[*TestModel]("idx", rawMessage)
		require.NoError(t, err)
		assert.True(t, opts.DocAsUpsert, "DocAsUpsert must default to true via NewBulkOptions")
	})

	t.Run("explicit false sticks", func(t *testing.T) {
		t.Parallel()

		opts, err := NewBulkOptions(
			"idx",
			rawMessage,
			WithDocAsUpsert[*TestModel](false),
		)
		require.NoError(t, err)
		assert.False(t, opts.DocAsUpsert,
			"WithDocAsUpsert(false) must not be flipped back to true (B9): update-only bulk ops would silently upsert")
	})

	t.Run("explicit true sticks", func(t *testing.T) {
		t.Parallel()

		opts, err := NewBulkOptions(
			"idx",
			rawMessage,
			WithDocAsUpsert[*TestModel](true),
		)
		require.NoError(t, err)
		assert.True(t, opts.DocAsUpsert)
	})
}

//////
// Unit: BulkCreate input validation (B8).
//////

// TestBulkCreate_EmptySampleDoc_StructLiteral pins B8: a caller-constructed
// &BulkOptions{} with an empty-but-non-nil SampleDoc passes the
// `validate:"required"` tag and used to reach `targetBatchSizeBytes /
// docSize` with docSize == 0 — an integer divide-by-zero panic.
func TestBulkCreate_EmptySampleDoc_StructLiteral(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	for _, tc := range []struct {
		name      string
		sampleDoc json.RawMessage
	}{
		{"empty non-nil RawMessage", json.RawMessage{}},
		{"nil RawMessage", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error

			require.NotPanics(t, func() {
				err = e.BulkCreate(
					context.Background(),
					testModelGenerator(2),
					&BulkOptions[*TestModel]{
						Index:     "audit-b8",
						SampleDoc: tc.sampleDoc,
					},
				)
			}, "BulkCreate must not panic on an empty SampleDoc (B8)")

			require.Error(t, err, "empty SampleDoc must surface as an error, not a divide-by-zero panic")
		})
	}
}

//////
// Unit: HTTP error responses surfaced (B13).
//////

// TestBulkCreate_NodeStats500_ErrorSurfaces pins B13: a 401/403/500 from
// /_nodes/stats used to decode into a zero NodesStats and silently produce
// bogus pressure numbers and worker counts.
func TestBulkCreate_NodeStats500_ErrorSurfaces(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{
		nodesStatsStatus: http.StatusInternalServerError,
		nodesStatsBody:   `{"error":{"type":"security_exception","reason":"forced"},"status":500}`,
	})
	e := newTestEBI(t, f.srv)

	opts, err := NewBulkOptions(
		"audit-b13",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(2), opts)
	require.Error(t, err,
		"a 500 from /_nodes/stats must surface as an error, not decode into zero stats (B13)")
}

// TestNew_PingErrorStatus_ReturnsError pins B13 on the factory: New checked
// the Ping transport error but not res.IsError(), so a 401/403/503 cluster
// yielded a "working" client.
func TestNew_PingErrorStatus_ReturnsError(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{pingStatus: http.StatusForbidden})

	_, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{Addresses: []string{f.srv.URL}},
	)
	require.Error(t, err, "New must fail when Ping returns an HTTP error status (B13)")
}

//////
// E2E: metrics channel behavior (B1/B2/B3/B4).
//////

// TestBulkCreate_MetricsSnapshots_AreCopies pins B3/B4: every MetricsCh
// send used to push the LIVE *Metrics pointer, so receivers reading (or
// json.Marshal-ing) fields raced with writers mutating under the mutex —
// and every receive aliased the same struct.
//
// The reader here marshals each snapshot WHILE indexing runs; under -race
// the pre-fix code trips. Post-fix, every received snapshot must be a
// distinct copy.
func TestBulkCreate_MetricsSnapshots_AreCopies(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	metricsCh := make(chan *Metrics, 4096)
	errorCh := make(chan error, 4096)

	// The reader is stopped via a signal channel, NOT by closing
	// metricsCh: the metrics goroutine winds down asynchronously after
	// BulkCreate returns, and closing a channel a producer may still be
	// sending on is a caller-contract violation.
	stopReader := make(chan struct{})
	readerDone := make(chan struct{})

	var received []*Metrics

	go func() {
		defer close(readerDone)

		for {
			select {
			case m := <-metricsCh:
				// Reading fields concurrently with the indexing writers
				// is exactly what production metrics consumers do.
				if _, err := json.Marshal(m); err != nil {
					t.Errorf("snapshot marshal failed: %v", err)
				}

				received = append(received, m)
			case <-stopReader:
				return
			}
		}
	}()

	opts, err := NewBulkOptions(
		"audit-snapshots",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		WithMetricsCh[*TestModel](metricsCh),
		WithErrorCh[*TestModel](errorCh),
		WithMetricsCheck[*TestModel](5*time.Millisecond),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(500), opts)
	require.NoError(t, err)

	// Give the metrics goroutine's wind-down a moment to deliver
	// stragglers, then stop the reader.
	time.Sleep(100 * time.Millisecond)
	close(stopReader)
	<-readerDone

	for drained := false; !drained; {
		select {
		case err := <-errorCh:
			t.Errorf("unexpected async error: %v", err)
		default:
			drained = true
		}
	}

	require.NotEmpty(t, received, "at least one metrics snapshot must be delivered")

	// Snapshots must be independent copies: mutating one received snapshot
	// must not affect any other. Aliased pointers (the pre-fix behavior —
	// every send pushed the same live *Metrics) fail this check.
	seen := make(map[*Metrics]bool, len(received))
	for i, m := range received {
		if seen[m] {
			t.Fatalf("snapshot %d aliases a previously received snapshot: MetricsCh must carry copies (B4)", i)
		}
		seen[m] = true
	}

	// The final snapshot must reflect actual progress.
	last := received[len(received)-1]
	assert.Positive(t, last.DocsProcessed, "final snapshot must reflect processed docs")
}

// TestBulkCreate_UnbufferedMetricsChNoReader_Completes pins B1/B2/B3: with
// an unbuffered MetricsCh and NO reader, BulkCreate used to park forever in
// its epilogue on the per-document deferred sends (one blocking send per
// doc, all deferred to return time).
func TestBulkCreate_UnbufferedMetricsChNoReader_Completes(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	metricsCh := make(chan *Metrics) // Unbuffered, nobody reads it. Ever.

	opts, err := NewBulkOptions(
		"audit-deadlock",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		WithMetricsCh[*TestModel](metricsCh),
		WithMetricsCheck[*TestModel](5*time.Millisecond),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(50), opts)
	require.NoError(t, err,
		"BulkCreate must complete even when nobody reads MetricsCh: metrics are best-effort telemetry (B1/B2/B3)")
}

// TestBulkCreate_MetricsGoroutineNoLeak_DeadReaders pins B2/B12: with a
// MetricsCh nobody drains and an unbuffered ErrorCh whose reader is gone,
// the metrics goroutine used to park forever — either in its accumulated
// deferred MetricsCh sends (B2) or in asyncErrorSend guarded by the CALLER
// ctx (Background here, never fires) instead of the goroutine ctx (B12).
//
// The failing /_refresh + always-true RefreshFunc force an error send from
// INSIDE the metrics goroutine on every tick, exercising the B12 path.
// Extends the leak tests in ebi_leak_test.go, which only cover nil
// MetricsCh/ErrorCh.
func TestBulkCreate_MetricsGoroutineNoLeak_DeadReaders(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping goroutine-count test in -short mode")
	}

	f := newCaptureES(t, captureESConfig{refreshStatus: http.StatusInternalServerError})
	e := newTestEBI(t, f.srv)

	docs := testModelGenerator(20_000)

	runOnce := func() error {
		metricsCh := make(chan *Metrics, 1) // Fills once, then nobody reads.
		errorCh := make(chan error)         // Unbuffered, reader already gone.

		opts, err := NewBulkOptions(
			"audit-leak",
			rawMessage,
			WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
			WithMetricsCh[*TestModel](metricsCh),
			WithErrorCh[*TestModel](errorCh),
			WithMetricsCheck[*TestModel](time.Millisecond),
			WithRefreshFunc[*TestModel](func(context.Context, *Metrics) bool { return true }),
		)
		if err != nil {
			return err
		}

		return runBulkCreateGuarded(t, context.Background(), e, docs, opts)
	}

	// Warm up lazy package state before sampling the baseline.
	require.NoError(t, runOnce())

	settleAndGC := func() {
		runtime.GC()
		runtime.Gosched()
		time.Sleep(300 * time.Millisecond)
		runtime.GC()
	}
	settleAndGC()
	baseline := runtime.NumGoroutine()

	const runs = 6
	for i := 0; i < runs; i++ {
		require.NoError(t, runOnce())
	}

	settleAndGC()
	after := runtime.NumGoroutine()

	// Same slack rationale as TestBulkCreate_NoMetricsGoroutineLeak_
	// AfterReturn: a real leak shows ~+1 per run, well past +5.
	if after > baseline+5 {
		t.Errorf("metrics goroutine leaked with dead channel readers: baseline=%d after=%d (runs=%d)",
			baseline, after, runs)
	}
}

// TestBulkCreate_CtxCancel_DeadReaders_ReturnsPromptly pins B1/B12 on the
// cancellation path: cancel mid-run with unbuffered channels and dead
// readers must return ErrContextCancelled promptly — the pre-fix code hung
// in the accumulated deferred MetricsCh sends on the return path.
func TestBulkCreate_CtxCancel_DeadReaders_ReturnsPromptly(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	metricsCh := make(chan *Metrics) // Unbuffered, no reader.
	errorCh := make(chan error)      // Unbuffered, no reader.

	// PauseFunc keeps the run alive deterministically: the first metrics
	// tick flips the status to paused, after which every loop iteration
	// sleeps PauseDuration — plenty of runway to cancel mid-run.
	opts, err := NewBulkOptions(
		"audit-cancel",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		WithMetricsCh[*TestModel](metricsCh),
		WithErrorCh[*TestModel](errorCh),
		WithMetricsCheck[*TestModel](time.Millisecond),
		WithPauseFunc[*TestModel](func(*Metrics) bool { return true }),
		WithPauseDuration[*TestModel](5*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- e.BulkCreate(ctx, testModelGenerator(20_000), opts)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "context cancelled",
			"cancellation mid-run must surface as ErrContextCancelled")
	case <-time.After(auditTimeout):
		t.Fatal("deadlock: BulkCreate did not return after ctx cancel with dead channel readers (B1/B12)")
	}
}

//////
// E2E: stats read before final flush (B6).
//////

// TestBulkCreate_FailuresInFinalFlush_ReturnError pins B6: with the default
// FlushBytes (~5MB) and a small doc set, the ONLY flush happens inside
// Close — which used to run AFTER Stats() was read, so per-item failures
// never reached the NumFailed check and BulkCreate returned nil.
func TestBulkCreate_FailuresInFinalFlush_ReturnError(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{failBulkItems: true})
	e := newTestEBI(t, f.srv)

	opts, err := NewBulkOptions(
		"audit-b6",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(10), opts)
	require.Error(t, err,
		"BulkCreate must report documents that failed during the final flush (B6)")

	// The failures must have actually reached ES (i.e. the flush happened
	// before the check, not skipped entirely).
	require.NotEmpty(t, f.bulkRequests(), "the final flush must have hit /_bulk")
}

//////
// E2E: opts wiring and immutability (B10/B10a/B11).
//////

// TestBulkCreate_FlushBytesRespected pins B10a: a caller-set FlushBytes was
// unconditionally overwritten with ~5MB, collapsing everything into a
// single bulk request.
func TestBulkCreate_FlushBytesRespected(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	opts, err := NewBulkOptions(
		"audit-flushbytes",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		WithFlushBytes[*TestModel](256),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(100), opts)
	require.NoError(t, err)

	// 100 docs at ~100 bytes each (meta+source) against FlushBytes=256
	// must produce many bulk requests; the pre-fix ~5MB override produced
	// exactly one.
	got := len(f.bulkRequests())
	assert.Greater(t, got, 1,
		"WithFlushBytes(256) must split 100 docs across multiple bulk requests (B10a); got %d request(s)", got)
}

// TestBulkCreate_BatchSizeDrivesFlush pins B10b: WithBatchSize was computed
// and then wired to NOTHING (HyperparameterOptimization was "optimizing" a
// dead parameter). With FlushBytes unset, a caller-set BatchSize must drive
// the flush cadence.
func TestBulkCreate_BatchSizeDrivesFlush(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	opts, err := NewBulkOptions(
		"audit-batchsize",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		WithBatchSize[*TestModel](3),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(30), opts)
	require.NoError(t, err)

	// BatchSize=3 with a ~24-byte sample doc means a tiny flush threshold:
	// 30 docs must span multiple bulk requests. The pre-fix dead parameter
	// left the ~5MB default, collapsing everything into a single request.
	got := len(f.bulkRequests())
	assert.Greater(t, got, 1,
		"WithBatchSize must drive the flush cadence when FlushBytes is unset (B10b); got %d request(s)", got)
}

// TestDiscoverWorkerNodes_NilNodeEntry pins the nil-entry guard on the
// discovery path: a `"nodes": {"x": null}` payload decodes into a nil
// *NodeDetails, which must be skipped, not dereferenced.
func TestDiscoverWorkerNodes_NilNodeEntry(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{
		nodesStatsStatus: http.StatusOK,
		nodesStatsBody: `{
			"_nodes":{"total":2,"successful":2,"failed":0},
			"cluster_name":"test-cluster",
			"nodes":{"a":null,"b":{"roles":["data","ingest"]}}
		}`,
	})
	e := newTestEBI(t, f.srv)

	n, err := e.discoverWorkerNodes(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only the non-nil data node must be counted")
}

// TestBulkCreate_OptsNotMutated_IndexNameFuncNotCompounded pins B10:
// BulkCreate used to write the IndexNameFunc result back into opts.Index,
// so reusing opts re-suffixed the index name on every call
// ("base-suffix-suffix"). HyperparameterOptimization reuses opts across
// every iteration, so this corrupted every run after the first.
func TestBulkCreate_OptsNotMutated_IndexNameFuncNotCompounded(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	opts, err := NewBulkOptions(
		"base",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		WithIndexNameFunc[*TestModel](func(indexName string) string {
			return indexName + "-suffix"
		}),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)

	for run := 0; run < 2; run++ {
		err := runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(5), opts)
		require.NoError(t, err, "run %d", run)
	}

	assert.Equal(t, "base", opts.Index,
		"BulkCreate must not write the derived index name back into opts (B10)")

	actions := f.allActions()
	require.NotEmpty(t, actions)

	for i, a := range actions {
		assert.Equal(t, "base-suffix", a.Index,
			"action %d: index name must be derived fresh per call, never compounded (B10)", i)
	}
}

// TestBulkCreate_ConcurrentCallsSharingOpts pins the B10 data race: two
// concurrent BulkCreate calls sharing one opts used to race on the plain
// writes to opts.BatchSize/NumWorkers/FlushBytes/Index (caught by -race).
// The struct-literal variant additionally pins the process() defaults
// application, which must happen on a private copy, not the shared struct.
func TestBulkCreate_ConcurrentCallsSharingOpts(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, opts *BulkOptions[*TestModel]) {
		t.Helper()

		f := newCaptureES(t, captureESConfig{})
		e := newTestEBI(t, f.srv)

		const callers = 2

		done := make(chan error, callers)

		for i := 0; i < callers; i++ {
			go func() {
				done <- e.BulkCreate(context.Background(), testModelGenerator(500), opts)
			}()
		}

		for i := 0; i < callers; i++ {
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(auditTimeout):
				t.Fatal("concurrent BulkCreate calls sharing opts did not finish in time")
			}
		}
	}

	t.Run("opts from NewBulkOptions", func(t *testing.T) {
		t.Parallel()

		opts, err := NewBulkOptions(
			"audit-concurrent",
			rawMessage,
			WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		)
		require.NoError(t, err)

		run(t, opts)
	})

	t.Run("struct-literal opts", func(t *testing.T) {
		t.Parallel()

		// Zero-valued default-tagged fields (MetricsCheck, FlushInterval,
		// Operation, ...) force process() to apply defaults on BOTH calls.
		opts := &BulkOptions[*TestModel]{
			Index:     "audit-concurrent-literal",
			SampleDoc: rawMessage,
		}

		run(t, opts)

		// The caller's struct must come out untouched: defaults are
		// applied to a private copy.
		assert.Zero(t, opts.MetricsCheck, "process() defaults must not be written into the caller's opts")
		assert.Zero(t, opts.Operation, "process() defaults must not be written into the caller's opts")
	})
}

// TestBulkCreate_RefreshPolicyWired pins B11: RefreshPolicy was accepted
// and validated but never passed to esutil, so ES never saw a refresh
// parameter.
func TestBulkCreate_RefreshPolicyWired(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		policy RefreshPolicy
		want   string
	}{
		// ES bulk accepts "true"/"false"/"wait_for": the exported
		// "immediate" constant must be mapped to "true".
		{"wait_for passes through", RefreshPolicyWaitFor, "wait_for"},
		{"immediate maps to true", RefreshPolicyImmediate, "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newCaptureES(t, captureESConfig{})
			e := newTestEBI(t, f.srv)

			opts, err := NewBulkOptions(
				"audit-refresh",
				rawMessage,
				WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
				WithRefreshPolicy[*TestModel](tc.policy),
				WithNumWorkers[*TestModel](NumWorkersManual(1)),
			)
			require.NoError(t, err)

			err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(5), opts)
			require.NoError(t, err)

			reqs := f.bulkRequests()
			require.NotEmpty(t, reqs)

			for i, r := range reqs {
				assert.Equal(t, tc.want, r.Query.Get("refresh"),
					"bulk request %d must carry the refresh policy (B11)", i)
			}
		})
	}
}

// TestBulkCreate_FlushCallbacksWired pins B11: FlushStartFunc/FlushEndFunc
// existed on BulkOptions with esutil-compatible signatures but were never
// passed to the bulk indexer.
func TestBulkCreate_FlushCallbacksWired(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	var starts, ends atomic.Int64

	opts, err := NewBulkOptions(
		"audit-flush-callbacks",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		WithFlushStartFunc[*TestModel](func(ctx context.Context) context.Context {
			starts.Add(1)
			return ctx
		}),
		WithFlushEndFunc[*TestModel](func(context.Context) {
			ends.Add(1)
		}),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(10), opts)
	require.NoError(t, err)

	assert.Positive(t, starts.Load(), "FlushStartFunc must be invoked at least once (B11)")
	assert.Positive(t, ends.Load(), "FlushEndFunc must be invoked at least once (B11)")
}

//////
// E2E: update/delete operations (B9 end-to-end + happy/bad paths).
//////

// TestBulkCreate_UpdateOperation_DocAsUpsertBody pins B9 end-to-end: the
// bulk NDJSON body for update operations must include "doc_as_upsert":true
// by default and OMIT it entirely with WithDocAsUpsert(false).
func TestBulkCreate_UpdateOperation_DocAsUpsertBody(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, wantUpsert bool, extra ...BulkOptionsFunc[*TestModel]) []bulkAction {
		t.Helper()

		f := newCaptureES(t, captureESConfig{})
		e := newTestEBI(t, f.srv)

		options := append([]BulkOptionsFunc[*TestModel]{
			WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
			WithOperation[*TestModel]("update"),
			WithNumWorkers[*TestModel](NumWorkersManual(1)),
		}, extra...)

		opts, err := NewBulkOptions("audit-update", rawMessage, options...)
		require.NoError(t, err)
		require.Equal(t, wantUpsert, opts.DocAsUpsert)

		err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(3), opts)
		require.NoError(t, err)

		actions := f.allActions()
		require.NotEmpty(t, actions)

		return actions
	}

	t.Run("default true includes doc_as_upsert", func(t *testing.T) {
		t.Parallel()

		for i, a := range run(t, true) {
			require.Equal(t, "update", a.Op)

			var body map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(a.Source, &body))

			assert.Contains(t, body, "doc", "action %d must wrap the document in a doc field", i)
			assert.Contains(t, body, "doc_as_upsert", "action %d must carry doc_as_upsert by default", i)
			assert.Equal(t, "true", string(body["doc_as_upsert"]), "action %d", i)
		}
	})

	t.Run("explicit false omits doc_as_upsert", func(t *testing.T) {
		t.Parallel()

		for i, a := range run(t, false, WithDocAsUpsert[*TestModel](false)) {
			require.Equal(t, "update", a.Op)

			var body map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(a.Source, &body))

			assert.Contains(t, body, "doc", "action %d must wrap the document in a doc field", i)
			assert.NotContains(t, body, "doc_as_upsert",
				"action %d: WithDocAsUpsert(false) must omit doc_as_upsert entirely (B9) — updates on missing docs must error, not upsert", i)
		}
	})
}

// TestBulkCreate_DeleteOperation_NoBody covers the delete happy path: the
// bulk body carries only metadata lines, no sources.
func TestBulkCreate_DeleteOperation_NoBody(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	opts, err := NewBulkOptions(
		"audit-delete",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		WithOperation[*TestModel]("delete"),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(5), opts)
	require.NoError(t, err)

	actions := f.allActions()
	require.Len(t, actions, 5)

	for i, a := range actions {
		assert.Equal(t, "delete", a.Op, "action %d", i)
		assert.Nil(t, a.Source, "action %d: delete operations must not carry a body", i)
	}
}

// TestBulkCreate_InvalidOperation covers the bad path: an unknown Operation
// must fail fast with ErrInvalidOperation.
func TestBulkCreate_InvalidOperation(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	opts, err := NewBulkOptions(
		"audit-bad-op",
		rawMessage,
		WithOperation[*TestModel]("bogus"),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(2), opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "operation")
}
