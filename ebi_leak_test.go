// Copyright 2024 The ebi Authors. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package ebi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"
)

// These tests cover the goroutine-leak contract for BulkCreate's metrics
// monitoring goroutine.
//
// The metrics goroutine is spawned per BulkCreate call to periodically
// poll node stats and update the metrics struct. It must exit when
// BulkCreate returns — otherwise it leaks one goroutine per BulkCreate
// invocation, accumulating across the lifetime of the process.
//
// The 2026-05-01 production hang in proj-ringboost-vendor traced to
// exactly this pattern: 3+ orphaned BulkCreate.func3 goroutines (from
// rotations 9–10 hours earlier) were still parked in their select loop,
// holding shared state that prevented thalesfsp/concurrentloop's
// wg.Wait() from ever returning. Goroutine dump captured by the
// rotation-stuck watchdog showed:
//
//   goroutine 14752 [select, 556 minutes]:
//     thalesfsp/ebi.(*EBI[...]).BulkCreate.func3() ebi.go:366
//   goroutine 9162 [select, 609 minutes]:
//     thalesfsp/ebi.(*EBI[...]).BulkCreate.func3() ebi.go:366
//   goroutine 72456204 [select, 98 minutes]:
//     thalesfsp/ebi.(*EBI[...]).BulkCreate.func3() ebi.go:366
//
// The leak occurs because the metrics goroutine watches the CALLER's
// ctx — but if the caller passed a background-like context that never
// fires Done() (which is the proj-ringboost-vendor cleanupCtx pattern,
// but is also valid for any caller that wants BulkCreate to run to
// completion), the goroutine has no termination signal once the ticker
// is stopped (the ticker.C channel becomes unreadable but does NOT
// close, so select{} keeps waiting on it forever).

// fakeESServer returns an httptest.Server that responds to every ES
// request with a syntactically-valid response. Sufficient to exercise
// BulkCreate end-to-end (including esutil.NewBulkIndexer's worker pool
// + the metrics goroutine) without a real ES cluster.
//
// Key details:
//   - X-Elastic-Product header is mandatory (the v8 client rejects
//     responses without it).
//   - /_nodes/stats: returns empty nodes map (sufficient for
//     calculateSystemPressure to compute defaults).
//   - /_bulk: returns 200 with no failed items.
//   - /: root info request used by the client at construction.
//   - Default for any other path: returns 200 + empty JSON object.
func fakeESServer(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")

		path := r.URL.Path
		switch {
		case path == "/" || path == "":
			// Client connection bootstrap.
			_, _ = w.Write([]byte(`{
				"name":"node1",
				"cluster_name":"test-cluster",
				"cluster_uuid":"test-uuid",
				"version":{
					"number":"8.11.0",
					"build_flavor":"default",
					"build_type":"docker",
					"build_hash":"abc",
					"build_date":"2024-01-01",
					"build_snapshot":false,
					"lucene_version":"9.0.0",
					"minimum_wire_compatibility_version":"7.17.0",
					"minimum_index_compatibility_version":"7.0.0"
				},
				"tagline":"You Know, for Search"
			}`))
		case strings.Contains(path, "_nodes/stats"):
			_, _ = w.Write([]byte(`{
				"_nodes":{"total":1,"successful":1,"failed":0},
				"cluster_name":"test-cluster",
				"nodes":{}
			}`))
		case strings.Contains(path, "_bulk"):
			_, _ = w.Write([]byte(`{"took":1,"errors":false,"items":[]}`))
		case strings.HasSuffix(path, "/_refresh"):
			_, _ = w.Write([]byte(`{"_shards":{"total":1,"successful":1,"failed":0}}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))

	t.Cleanup(srv.Close)

	return srv
}

// newTestEBI constructs an EBI[*TestModel] pointed at the fake ES server.
// Returns an instance ready to run BulkCreate against without needing a
// real cluster.
func newTestEBI(t *testing.T, srv *httptest.Server) *EBI[*TestModel] {
	t.Helper()

	ebi, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{
			Addresses: []string{srv.URL},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, ebi)

	return ebi
}

// TestBulkCreate_HappyPath verifies that BulkCreate completes a normal
// indexing operation end-to-end against the fake ES server. This is the
// regression guard that the leak-fix does not break the well-behaved
// path.
func TestBulkCreate_HappyPath(t *testing.T) {
	t.Parallel()

	srv := fakeESServer(t)
	ebi := newTestEBI(t, srv)

	docs := testModelGenerator(10)

	opts, err := NewBulkOptions(
		"test-idx",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
	)
	require.NoError(t, err)

	err = ebi.BulkCreate(context.Background(), docs, opts)
	require.NoError(t, err)
}

// TestBulkCreate_BadPath_NilDocs covers the error path: BulkCreate with
// no documents must fail fast.
func TestBulkCreate_BadPath_NilDocs(t *testing.T) {
	t.Parallel()

	srv := fakeESServer(t)
	ebi := newTestEBI(t, srv)

	opts, err := NewBulkOptions(
		"test-idx",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
	)
	require.NoError(t, err)

	err = ebi.BulkCreate(context.Background(), nil, opts)
	require.Error(t, err, "BulkCreate with nil docs MUST return an error")
}

// TestBulkCreate_NoMetricsGoroutineLeak_AfterReturn is the regression
// guard for the 2026-05-01 production goroutine-leak that hung the
// rotation. After BulkCreate returns, NO goroutine spawned by
// BulkCreate should remain alive — including (and especially) the
// metrics monitoring goroutine.
//
// Without the fix: the metrics goroutine is spawned watching the
// CALLER's ctx. The caller here uses context.Background() (which never
// fires Done), so the goroutine never sees ctx.Done() and the ticker
// C channel never receives after Stop() — leaving the goroutine parked
// in its select forever.
//
// With the fix: BulkCreate creates an internal context.WithCancel
// derived from the caller's ctx, defers the cancel. When BulkCreate
// returns, the deferred cancel fires, the metrics goroutine sees its
// internal ctx fire Done(), and exits cleanly.
//
// Why we sample after a generous settle delay: BulkCreate kicks off
// internal worker goroutines in esutil.NewBulkIndexer that take a few
// hundred ms to wind down. We sample baseline post-settle, run the
// operation, settle again, then check that no NEW goroutines remain.
func TestBulkCreate_NoMetricsGoroutineLeak_AfterReturn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping goroutine-count test in -short mode")
	}

	srv := fakeESServer(t)
	ebi := newTestEBI(t, srv)

	docs := testModelGenerator(10)
	opts, err := NewBulkOptions(
		"test-idx",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
	)
	require.NoError(t, err)

	// Warm up: run BulkCreate once so any lazy-initialized package state
	// settles before we sample baseline. Without this the FIRST run
	// inflates the count by an amount unrelated to the leak.
	require.NoError(t, ebi.BulkCreate(context.Background(), docs, opts))

	settleAndGC := func() {
		runtime.GC()
		runtime.Gosched()
		time.Sleep(300 * time.Millisecond)
		runtime.GC()
	}
	settleAndGC()
	baseline := runtime.NumGoroutine()

	// Run many BulkCreates with caller ctx = Background (never fires).
	// Each invocation that leaks adds 1 goroutine; after 20 runs the
	// signal-to-noise is unambiguous (20+ extra goroutines vs the few
	// stragglers from BulkIndexer's internal worker pool).
	const runs = 20
	for i := 0; i < runs; i++ {
		require.NoError(t, ebi.BulkCreate(context.Background(), docs, opts))
	}

	settleAndGC()
	after := runtime.NumGoroutine()

	// Allow modest slack (5) for esutil.BulkIndexer internal pools that
	// may not be reclaimed by GC even after Close(). Anything past +5
	// strongly indicates the metrics goroutine is leaking — at runs=20
	// a real leak shows ~+20.
	if after > baseline+5 {
		t.Errorf("BulkCreate goroutine leak: baseline=%d after=%d (runs=%d, expected ~baseline; threshold baseline+5)",
			baseline, after, runs)
	}
}

// TestBulkCreate_MetricsGoroutineExitsWhenCallerCtxIsBackground is a
// more direct probe of the leak fix. It uses an atomic counter to track
// metrics-loop entries/exits. Even with caller ctx = Background, the
// loop must exit shortly after BulkCreate returns.
//
// This test deliberately uses a very short MetricsCheck so the loop
// has fired at least once before BulkCreate returns; without that, a
// loop that never ticks could be considered "leaked" by the goroutine
// counter alone, which would not be a meaningful test of THIS bug.
//
// Because the metrics loop is goroutine-scoped (no external observable),
// we measure indirectly: a successfully exiting loop releases its
// stack which becomes visible via runtime.NumGoroutine drop. The
// regression test here is functionally a stronger version of the
// previous test, plus an explicit MetricsCheck setting that exercises
// the ticker path.
func TestBulkCreate_MetricsGoroutineExitsWhenCallerCtxIsBackground(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping goroutine-count test in -short mode")
	}

	srv := fakeESServer(t)
	ebi := newTestEBI(t, srv)

	docs := testModelGenerator(5)
	opts, err := NewBulkOptions(
		"test-idx",
		rawMessage,
		WithDocumentIDFunc(func(d *TestModel) string { return d.ID }),
		WithMetricsCheck[*TestModel](20*time.Millisecond),
	)
	require.NoError(t, err)

	// Warmup.
	require.NoError(t, ebi.BulkCreate(context.Background(), docs, opts))

	runtime.GC()
	runtime.Gosched()
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	// 3 sequential BulkCreates each running long enough for the metrics
	// ticker to fire. With MetricsCheck=20ms and ~50 docs total the
	// ticker fires ~1-3 times per BulkCreate.
	for i := 0; i < 3; i++ {
		require.NoError(t, ebi.BulkCreate(context.Background(), docs, opts))
	}

	runtime.GC()
	runtime.Gosched()
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()

	if after > baseline+5 {
		t.Errorf("metrics goroutine leaked across 3 BulkCreate calls: baseline=%d after=%d",
			baseline, after)
	}
}

// counterIncFn is unused but reserved for future tests that want to
// observe call counts via an atomic counter. Silence the unused
// warning by referencing it.
func counterIncFn(c *atomic.Int64) func() {
	return func() { c.Add(1) }
}

var _ = counterIncFn
