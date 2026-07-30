package ebi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/ho"
)

type testUpdateEnvelope[T any] struct {
	Doc         T    `json:"doc"`
	DocAsUpsert bool `json:"doc_as_upsert,omitempty"`
}

type bulkRoutingRecorder struct {
	base http.RoundTripper

	mu       sync.Mutex
	routings []string
}

func (r *bulkRoutingRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil && strings.Contains(req.URL.Path, "_bulk") {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}

		if err := req.Body.Close(); err != nil {
			return nil, err
		}

		req.Body = io.NopCloser(bytes.NewReader(body))

		for _, line := range bytes.Split(body, []byte("\n")) {
			var action map[string]struct {
				Routing string `json:"routing"`
			}

			if err := json.Unmarshal(line, &action); err != nil {
				continue
			}

			for _, operation := range []string{Index, Create, Update, Delete} {
				if metadata, ok := action[operation]; ok {
					r.mu.Lock()
					r.routings = append(r.routings, metadata.Routing)
					r.mu.Unlock()

					break
				}
			}
		}
	}

	return r.base.RoundTrip(req)
}

func (r *bulkRoutingRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.routings...)
}

func TestNumWorkersByJVM(t *testing.T) {
	t.Parallel()

	metrics, err := NewMetrics()
	require.NoError(t, err)

	workers, err := NumWorkersByJVM(metrics)()
	require.NoError(t, err)
	assert.Equal(t, 1, workers, "zero node RAM must retain at least one worker")

	metrics.updateComputed(0, 0, 0, 1, 16)

	workers, err = NumWorkersByJVM(metrics)()
	require.NoError(t, err)
	assert.Equal(t, 8, workers, "half of 16 GiB is an 8 GiB heap, yielding eight workers")
}

func TestNumWorkersAutoDiscovery(t *testing.T) {
	t.Parallel()

	t.Run("counts only data roles including compound roles", func(t *testing.T) {
		t.Parallel()

		f := newCaptureES(t, captureESConfig{
			nodesStatsStatus: http.StatusOK,
			nodesStatsBody: `{
				"_nodes":{"total":5,"successful":5,"failed":0},
				"cluster_name":"test-cluster",
				"nodes":{
					"data":{"roles":["data","ingest"]},
					"hot":{"roles":["data_hot","ingest"]},
					"content":{"roles":["data_content"]},
					"master":{"roles":["master","ingest"]},
					"coordinator":{"roles":[]}
				}
			}`,
		})
		e := newTestEBI(t, f.srv)

		workers, err := NumWorkersAutoDiscovery(context.Background(), e)()
		require.NoError(t, err)
		assert.Equal(t, 3, workers)
	})

	t.Run("surfaces node stats HTTP errors", func(t *testing.T) {
		t.Parallel()

		f := newCaptureES(t, captureESConfig{
			nodesStatsStatus: http.StatusInternalServerError,
			nodesStatsBody:   `{"error":{"type":"forced","reason":"node stats failed"},"status":500}`,
		})
		e := newTestEBI(t, f.srv)

		workers, err := NumWorkersAutoDiscovery(context.Background(), e)()
		require.Error(t, err)
		assert.Zero(t, workers)
	})
}

func TestMustGetReturnsUsableCatalogError(t *testing.T) {
	t.Parallel()

	catalogError := MustGet(ErrDocumentsRequired)
	require.NotNil(t, catalogError)

	err := catalogError.NewRequiredError()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "documents")
}

func TestWithRoutingFuncAppearsInBulkMetadata(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	recorder := &bulkRoutingRecorder{base: http.DefaultTransport}

	e, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{
			Addresses: []string{f.srv.URL},
			Transport: recorder,
		},
	)
	require.NoError(t, err)

	docs := testModelGenerator(4)
	opts, err := NewBulkOptions(
		"routing-index",
		rawMessage,
		WithDocumentIDFunc(func(doc *TestModel) string { return doc.ID }),
		WithRoutingFunc(func(doc *TestModel) string { return "group-" + doc.ID }),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, docs, opts)
	require.NoError(t, err)
	require.Len(t, f.allActions(), len(docs), "all documents must reach the request-capturing fake")
	assert.ElementsMatch(t,
		[]string{"group-0", "group-1", "group-2", "group-3"},
		recorder.snapshot(),
	)
}

func TestBulkOptionsSetFlushIntervalAndRetryOnFailure(t *testing.T) {
	t.Parallel()

	opts, err := NewBulkOptions(
		"options-index",
		rawMessage,
		WithFlushInterval[*TestModel](275*time.Millisecond),
		WithRetryOnFailure[*TestModel](7),
	)
	require.NoError(t, err)
	assert.Equal(t, 275*time.Millisecond, opts.FlushInterval)
	assert.Equal(t, 7, opts.RetryOnFailure)
}

func TestNewFailurePaths(t *testing.T) {
	t.Parallel()

	t.Run("client constructor rejects addresses with cloud ID", func(t *testing.T) {
		t.Parallel()

		e, err := New[*TestModel](
			context.Background(),
			elasticsearch.Config{
				Addresses: []string{"http://127.0.0.1:9200"},
				CloudID:   "deployment:ZmFrZSQxMjcuMC4wLjEkMTI3LjAuMC4x",
			},
		)
		require.Error(t, err)
		assert.Nil(t, e)
		assert.Contains(t, err.Error(), "create client")
	})

	t.Run("unreachable address returns ping error", func(t *testing.T) {
		t.Parallel()

		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		address := srv.URL
		srv.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		e, err := New[*TestModel](
			ctx,
			elasticsearch.Config{Addresses: []string{address}},
		)
		require.Error(t, err)
		assert.Nil(t, e)
		assert.Contains(t, err.Error(), "ping")
	})
}

func TestBulkCreatePauseThenResumeCompletesEveryDocument(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	var pauseChecks atomic.Int32
	paused := make(chan struct{})
	running := make(chan struct{})
	var pausedOnce sync.Once
	var runningOnce sync.Once

	docs := testModelGenerator(20)
	opts, err := NewBulkOptions(
		"pause-index",
		rawMessage,
		WithMetricsCheck[*TestModel](time.Millisecond),
		WithPauseDuration[*TestModel](5*time.Millisecond),
		WithPauseFunc[*TestModel](func(*Metrics) bool {
			check := pauseChecks.Add(1)
			if check <= 2 {
				pausedOnce.Do(func() { close(paused) })

				return true
			}

			runningOnce.Do(func() { close(running) })

			return false
		}),
		WithDocumentIDFunc(func(doc *TestModel) string {
			<-paused

			return doc.ID
		}),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, docs, opts)
	require.NoError(t, err)

	select {
	case <-running:
	default:
		t.Fatal("pause callback never reported that processing could resume")
	}

	assert.GreaterOrEqual(t, pauseChecks.Load(), int32(3))
	assert.Len(t, f.allActions(), len(docs), "pausing must not drop documents")
}

func TestBulkCreateMetricsPollErrorDelivered(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	original := f.srv.Config.Handler

	var nodeStatsCalls atomic.Int32
	errorCh := make(chan error, 2)

	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "_nodes/stats"):
			if nodeStatsCalls.Add(1) > 1 {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"type":"forced","reason":"poll failed"},"status":500}`))

				return
			}
		case strings.Contains(r.URL.Path, "_bulk"):
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()

			for len(errorCh) == 0 {
				select {
				case <-ticker.C:
				case <-r.Context().Done():
					return
				}
			}
		}

		original.ServeHTTP(w, r)
	})

	e := newTestEBI(t, f.srv)
	opts, err := NewBulkOptions(
		"metrics-error-index",
		rawMessage,
		WithMetricsCheck[*TestModel](time.Millisecond),
		WithErrorCh[*TestModel](errorCh),
		WithFlushBytes[*TestModel](1),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(3), opts)
	require.NoError(t, err)

	select {
	case pollErr := <-errorCh:
		require.Error(t, pollErr)
		assert.Contains(t, pollErr.Error(), "update metrics")
	case <-time.After(time.Second):
		t.Fatal("metrics poll error was not delivered to the buffered ErrorCh")
	}

	assert.GreaterOrEqual(t, nodeStatsCalls.Load(), int32(2))
}

func TestBulkCreateRefreshFuncSendsRefreshRequest(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	original := f.srv.Config.Handler

	refreshSeen := make(chan struct{})
	var refreshOnce sync.Once
	var refreshRequests atomic.Int32

	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/_refresh"):
			refreshRequests.Add(1)
			refreshOnce.Do(func() { close(refreshSeen) })
		case strings.Contains(r.URL.Path, "_bulk"):
			select {
			case <-refreshSeen:
			case <-r.Context().Done():
				return
			}
		}

		original.ServeHTTP(w, r)
	})

	e := newTestEBI(t, f.srv)
	opts, err := NewBulkOptions(
		"refresh-index",
		rawMessage,
		WithMetricsCheck[*TestModel](time.Millisecond),
		WithRefreshFunc[*TestModel](func(context.Context, *Metrics) bool { return true }),
		WithFlushBytes[*TestModel](1),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)

	err = runBulkCreateGuarded(t, context.Background(), e, testModelGenerator(3), opts)
	require.NoError(t, err)
	assert.Greater(t, refreshRequests.Load(), int32(0), "RefreshFunc(true) must issue /_refresh")
}

func TestBulkCreateWorkerCountErrorSurfaces(t *testing.T) {
	t.Parallel()

	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)
	workerErr := errors.New("worker discovery failed")

	opts, err := NewBulkOptions(
		"worker-error-index",
		rawMessage,
		WithNumWorkers[*TestModel](func() (int, error) { return 0, workerErr }),
	)
	require.NoError(t, err)

	err = e.BulkCreate(context.Background(), testModelGenerator(1), opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "retrieve number of worker nodes")
	assert.ErrorIs(t, err, workerErr)
}

func TestHandleChannelBranches(t *testing.T) {
	t.Parallel()

	t.Run("nil callback consumes work and exits", func(t *testing.T) {
		t.Parallel()

		workCh := make(chan int, 1)
		workCh <- 42
		close(workCh)

		returned := make(chan struct{})
		go func() {
			HandleChannel(context.Background(), workCh, make(chan error, 1), make(chan struct{}), nil)
			close(returned)
		}()

		select {
		case <-returned:
			assert.Empty(t, workCh)
		case <-time.After(time.Second):
			t.Fatal("nil callback prevented HandleChannel from consuming and exiting")
		}
	})

	t.Run("nil error channel drops callback error", func(t *testing.T) {
		t.Parallel()

		workCh := make(chan int, 1)
		workCh <- 7
		close(workCh)

		var callbackCalls atomic.Int32
		returned := make(chan struct{})
		go func() {
			HandleChannel(
				context.Background(),
				workCh,
				nil,
				make(chan struct{}),
				func(value int) error {
					callbackCalls.Add(1)
					assert.Equal(t, 7, value)

					return errors.New("dropped by contract")
				},
			)
			close(returned)
		}()

		select {
		case <-returned:
			assert.Equal(t, int32(1), callbackCalls.Load())
		case <-time.After(time.Second):
			t.Fatal("nil error channel caused HandleChannel to hang")
		}
	})

	t.Run("done channel exits without invoking callback", func(t *testing.T) {
		t.Parallel()

		doneCh := make(chan struct{})
		close(doneCh)

		var callbackCalls atomic.Int32
		returned := make(chan struct{})
		go func() {
			HandleChannel(
				context.Background(),
				make(chan int),
				make(chan error),
				doneCh,
				func(int) error {
					callbackCalls.Add(1)

					return nil
				},
			)
			close(returned)
		}()

		select {
		case <-returned:
			assert.Zero(t, callbackCalls.Load())
		case <-time.After(time.Second):
			t.Fatal("closed done channel did not stop HandleChannel")
		}
	})
}

func TestUpdateEnvelopeMarshalMatchesMap(t *testing.T) {
	t.Parallel()

	doc := &TestModel{ID: "42", Group: 3, Something: "same bytes"}

	for _, docAsUpsert := range []bool{false, true} {
		typedBytes, err := json.Marshal(testUpdateEnvelope[*TestModel]{
			Doc:         doc,
			DocAsUpsert: docAsUpsert,
		})
		require.NoError(t, err)

		mapEnvelope := map[string]any{"doc": doc}
		if docAsUpsert {
			mapEnvelope["doc_as_upsert"] = true
		}

		mapBytes, err := json.Marshal(mapEnvelope)
		require.NoError(t, err)
		assert.Equal(t, mapBytes, typedBytes, "doc_as_upsert=%t", docAsUpsert)
	}
}

func TestMetricsScalarUpdatesAndAddProcessed(t *testing.T) {
	t.Parallel()

	metrics, err := NewMetrics()
	require.NoError(t, err)

	metrics.UpdateOverallPressure(11)
	metrics.UpdateMemoryPressure(22)
	metrics.UpdateWritePressure(33)
	metrics.AddProcessed(128)

	snapshot := metrics.GetMetrics()
	assert.Equal(t, float64(11), snapshot.OverallPressure)
	assert.Equal(t, float64(22), snapshot.MemoryPressure)
	assert.Equal(t, float64(33), snapshot.WritePressure)
	assert.Equal(t, int64(1), snapshot.DocsProcessed)
	assert.Equal(t, int64(128), snapshot.BytesProcessed)
	assert.Equal(t, StatusRunning, metrics.getStatus())
}

func TestHyperparameterOptimizationBehavior(t *testing.T) {
	t.Parallel()

	validRanges := []ho.ParameterRange[int]{
		{Min: 1, Max: 1},
		{Min: 1, Max: 1},
	}

	t.Run("validates options and ranges", func(t *testing.T) {
		t.Parallel()

		var e EBI[*TestModel]

		_, err := e.HyperparameterOptimization(
			context.Background(),
			testModelGenerator(1),
			nil,
			ho.OptimizationConfig{},
			validRanges,
		)
		require.Error(t, err)

		opts := &BulkOptions[*TestModel]{}
		_, err = e.HyperparameterOptimization(
			context.Background(),
			testModelGenerator(1),
			opts,
			ho.OptimizationConfig{},
			validRanges[:1],
		)
		require.Error(t, err)

		for _, ranges := range [][]ho.ParameterRange[int]{
			{{Min: 0, Max: 1}, {Min: 1, Max: 1}},
			{{Min: 2, Max: 1}, {Min: 1, Max: 1}},
		} {
			_, err = e.HyperparameterOptimization(
				context.Background(),
				testModelGenerator(1),
				opts,
				ho.OptimizationConfig{},
				ranges,
			)
			require.Error(t, err)
		}
	})

	t.Run("no trials returns an error without mutating options", func(t *testing.T) {
		t.Parallel()

		f := newCaptureES(t, captureESConfig{})
		e := newTestEBI(t, f.srv)
		opts, err := NewBulkOptions[*TestModel]("ho-no-trials", rawMessage)
		require.NoError(t, err)

		best, err := e.HyperparameterOptimization(
			context.Background(),
			testModelGenerator(1),
			opts,
			ho.OptimizationConfig{},
			validRanges,
		)
		require.Error(t, err)
		assert.Equal(t, []int{0, 0}, best)
		assert.Zero(t, opts.BatchSize)
		assert.Nil(t, opts.NumWorkers)
	})

	t.Run("failed trial includes its cause", func(t *testing.T) {
		t.Parallel()

		f := newCaptureES(t, captureESConfig{})
		e := newTestEBI(t, f.srv)
		opts, err := NewBulkOptions[*TestModel]("ho-failed", rawMessage)
		require.NoError(t, err)

		_, err = e.HyperparameterOptimization(
			context.Background(),
			nil,
			opts,
			ho.OptimizationConfig{InitialSamples: 1},
			validRanges,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "documents")
	})

	t.Run("successful trial returns fixed parameters without mutating options", func(t *testing.T) {
		t.Parallel()

		f := newCaptureES(t, captureESConfig{})
		e := newTestEBI(t, f.srv)
		opts, err := NewBulkOptions[*TestModel]("ho-success", rawMessage)
		require.NoError(t, err)

		best, err := e.HyperparameterOptimization(
			context.Background(),
			testModelGenerator(2),
			opts,
			ho.OptimizationConfig{InitialSamples: 1},
			validRanges,
		)
		require.NoError(t, err)
		assert.Equal(t, []int{1, 1}, best)
		assert.Zero(t, opts.BatchSize)
		assert.Nil(t, opts.NumWorkers)
		assert.Len(t, f.allActions(), 2)
	})
}
