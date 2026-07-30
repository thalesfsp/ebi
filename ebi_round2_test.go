// Copyright 2024 The ebi Authors. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package ebi

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/ho"
)

const round2Timeout = 10 * time.Second

type round2ESConfig struct {
	bulkStatus       int
	bulkBody         string
	bulkStarted      chan struct{}
	blockBulkUntilCX bool
	refreshBody      string
}

func newRound2ESServer(t *testing.T, cfg round2ESConfig) *httptest.Server {
	t.Helper()

	var bulkStartedOnce sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")

		switch {
		case r.URL.Path == "/" || r.URL.Path == "":
			if r.Method == http.MethodHead {
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
		case strings.Contains(r.URL.Path, "_nodes/stats"):
			_, _ = w.Write([]byte(`{
				"_nodes":{"total":1,"successful":1,"failed":0},
				"cluster_name":"test-cluster","nodes":{}
			}`))
		case strings.Contains(r.URL.Path, "_bulk"):
			if cfg.bulkStarted != nil {
				bulkStartedOnce.Do(func() { close(cfg.bulkStarted) })
			}

			if cfg.blockBulkUntilCX {
				// Hold the flush open so the caller's cancellation lands while a
				// bulk request is in flight. The deadline is a safety net, not
				// part of the scenario: esutil's final flush does not always
				// carry the cancelled context, and without a bound the handler
				// would never answer, wedging the worker that Close waits on.
				select {
				case <-r.Context().Done():
				case <-time.After(2 * time.Second):
				}

				return
			}

			if cfg.bulkStatus != 0 {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(cfg.bulkStatus)
				_, _ = w.Write([]byte(`{"error":{"type":"forced","reason":"forced bulk rejection"}}`))

				return
			}

			if cfg.bulkBody != "" {
				_, _ = io.Copy(io.Discard, r.Body)
				_, _ = w.Write([]byte(cfg.bulkBody))

				return
			}

			_, _ = w.Write(bulkSuccessResponse(r.Body))
		case strings.HasSuffix(r.URL.Path, "/_refresh"):
			body := cfg.refreshBody
			if body == "" {
				body = `{"_shards":{"total":1,"successful":1,"failed":0}}`
			}

			_, _ = w.Write([]byte(body))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))

	t.Cleanup(srv.Close)

	return srv
}

func newRound2TestEBI[T any](t *testing.T, srv *httptest.Server) *EBI[T] {
	t.Helper()

	e, err := New[T](
		context.Background(),
		elasticsearch.Config{Addresses: []string{srv.URL}},
	)
	require.NoError(t, err)
	require.NotNil(t, e)

	return e
}

func TestRound2BulkCreateFinalFlushErrorDoesNotDeadlock(t *testing.T) {
	f := newCaptureES(t, captureESConfig{failBulkItems: true})
	e := newTestEBI(t, f.srv)
	errorCh := make(chan error)

	opts, err := NewBulkOptions(
		"round2-final-flush",
		rawMessage,
		WithDocumentIDFunc(func(doc *TestModel) string { return doc.ID }),
		WithErrorCh[*TestModel](errorCh),
	)
	require.NoError(t, err)

	done := make(chan struct{})
	var bulkErr error

	go func() {
		defer close(done)

		bulkErr = e.BulkCreate(context.Background(), testModelGenerator(5), opts)
	}()

	select {
	case <-done:
	case <-time.After(round2Timeout):
		t.Fatal("BulkCreate did not return: final-flush error send deadlocked")
	}

	require.Error(t, bulkErr)
	require.NotEmpty(t, f.bulkRequests(), "documents must reach the final flush")
}

type round2MarshalModel struct {
	ID    string  `json:"id"`
	Value float64 `json:"value"`
}

func TestRound2BulkCreateMarshalFailuresAreReported(t *testing.T) {
	f := newCaptureES(t, captureESConfig{})
	e := newRound2TestEBI[*round2MarshalModel](t, f.srv)
	metricsCh := make(chan *Metrics, 4096)

	docs := []*round2MarshalModel{{ID: "bad", Value: math.NaN()}}
	for i := 0; i < 25; i++ {
		docs = append(docs, &round2MarshalModel{ID: "ok", Value: float64(i)})
	}

	_, marshalErr := json.Marshal(docs[0])
	require.Error(t, marshalErr, "the selected NaN document must be an actual json.Marshal failure")

	opts, err := NewBulkOptions(
		"round2-marshal",
		json.RawMessage(`{"id":"sample","value":1}`),
		WithDocumentIDFunc(func(doc *round2MarshalModel) string {
			time.Sleep(2 * time.Millisecond)

			return doc.ID
		}),
		WithErrorCh[*round2MarshalModel](nil),
		WithMetricsCh[*round2MarshalModel](metricsCh),
		WithMetricsCheck[*round2MarshalModel](time.Millisecond),
		WithNumWorkers[*round2MarshalModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)

	err = e.BulkCreate(context.Background(), docs, opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "marshalFailedDocs")
	require.Contains(t, err.Error(), "1")

	var maxFailed int64
	for {
		select {
		case snapshot := <-metricsCh:
			maxFailed = max(maxFailed, snapshot.DocsFailed)
		default:
			require.GreaterOrEqual(t, maxFailed, int64(1),
				"at least one metrics snapshot must report the marshal failure")

			return
		}
	}
}

func TestRound2BulkCreateFlushLevelFailures(t *testing.T) {
	t.Run("truncated bulk response returns an error", func(t *testing.T) {
		srv := newRound2ESServer(t, round2ESConfig{
			bulkBody: `{"took":1,"errors":false,"items":[`,
		})
		e := newTestEBI(t, srv)

		opts, err := NewBulkOptions(
			"round2-truncated",
			rawMessage,
			WithNumWorkers[*TestModel](NumWorkersManual(1)),
		)
		require.NoError(t, err)

		err = e.BulkCreate(context.Background(), testModelGenerator(3), opts)
		require.Error(t, err, "an undecodable HTTP 200 bulk response must fail the call")
	})

	t.Run("cancellation is not an indexer error", func(t *testing.T) {
		bulkStarted := make(chan struct{})
		srv := newRound2ESServer(t, round2ESConfig{
			bulkStarted:      bulkStarted,
			blockBulkUntilCX: true,
		})
		e := newTestEBI(t, srv)
		errorCh := make(chan error, 8)

		opts, err := NewBulkOptions(
			"round2-cancelled-flush",
			rawMessage,
			WithErrorCh[*TestModel](errorCh),
			WithFlushBytes[*TestModel](1),
			WithNumWorkers[*TestModel](NumWorkersManual(1)),
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)

		go func() {
			done <- e.BulkCreate(ctx, testModelGenerator(10), opts)
		}()

		select {
		case <-bulkStarted:
		case <-time.After(round2Timeout):
			cancel()
			t.Fatal("bulk request did not start")
		}

		cancel()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(round2Timeout):
			t.Fatal("BulkCreate did not return after cancellation")
		}

		select {
		case err := <-errorCh:
			t.Fatalf("cancellation must not be emitted as an indexer error: %v", err)
		default:
		}
	})
}

func TestRound2BulkCreateJoinsMetricsGoroutineBeforeReturn(t *testing.T) {
	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)
	docs := testModelGenerator(25)

	const iterations = 20

	for iteration := 0; iteration < iterations; iteration++ {
		metricsCh := make(chan *Metrics, 4096)

		opts, err := NewBulkOptions(
			"round2-no-send-after-return",
			rawMessage,
			WithDocumentIDFunc(func(doc *TestModel) string {
				time.Sleep(200 * time.Microsecond)

				return doc.ID
			}),
			WithMetricsCh[*TestModel](metricsCh),
			WithMetricsCheck[*TestModel](50*time.Microsecond),
			WithNumWorkers[*TestModel](NumWorkersManual(1)),
		)
		require.NoError(t, err)
		require.NoError(t, e.BulkCreate(context.Background(), docs, opts), "iteration %d", iteration)

		close(metricsCh)
		time.Sleep(time.Millisecond)
	}
}

func TestRound2BulkCreateRejectsNegativeWorkerCount(t *testing.T) {
	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)

	opts, err := NewBulkOptions(
		"round2-negative-workers",
		rawMessage,
		WithNumWorkers[*TestModel](NumWorkersManual(-1)),
	)
	require.NoError(t, err)

	var bulkErr error
	require.NotPanics(t, func() {
		bulkErr = e.BulkCreate(context.Background(), testModelGenerator(1), opts)
	})
	require.Error(t, bulkErr, "a negative manual worker count must be rejected")
}

func TestRound2NewBulkOptionsRejectsNegativeMetricsCheck(t *testing.T) {
	var (
		opts *BulkOptions[*TestModel]
		err  error
	)

	require.NotPanics(t, func() {
		opts, err = NewBulkOptions(
			"round2-negative-metrics-check",
			rawMessage,
			WithMetricsCheck[*TestModel](-time.Millisecond),
		)
	})
	require.Error(t, err)
	require.Nil(t, opts)
}

func TestRound2BulkCreateCancellationInterruptsPause(t *testing.T) {
	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)
	pauseObserved := make(chan struct{})
	var pauseOnce sync.Once
	var docIDCalls atomic.Int64

	const (
		pauseDuration = 2 * time.Second
		promptBound   = 300 * time.Millisecond
	)

	opts, err := NewBulkOptions(
		"round2-cancel-pause",
		rawMessage,
		WithDocumentIDFunc(func(doc *TestModel) string {
			if docIDCalls.Add(1) == 1 {
				<-pauseObserved
				time.Sleep(20 * time.Millisecond)
			}

			return doc.ID
		}),
		WithMetricsCheck[*TestModel](time.Millisecond),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
		WithPauseFunc[*TestModel](func(*Metrics) bool {
			pauseOnce.Do(func() { close(pauseObserved) })

			return true
		}),
		WithPauseDuration[*TestModel](pauseDuration),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- e.BulkCreate(ctx, testModelGenerator(10), opts)
	}()

	select {
	case <-pauseObserved:
	case <-time.After(round2Timeout):
		t.Fatal("PauseFunc was not exercised")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(promptBound):
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(pauseDuration + time.Second):
			t.Fatal("BulkCreate remained stuck beyond the configured pause")
		}

		t.Errorf("BulkCreate did not return within %s after cancellation; pause duration was %s",
			promptBound, pauseDuration)
	}
}

func TestRound2BulkCreatePublishesTerminalStatus(t *testing.T) {
	f := newCaptureES(t, captureESConfig{})
	e := newTestEBI(t, f.srv)
	metricsCh := make(chan *Metrics, 4096)

	opts, err := NewBulkOptions(
		"round2-terminal-status",
		rawMessage,
		WithMetricsCh[*TestModel](metricsCh),
		WithMetricsCheck[*TestModel](time.Millisecond),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	require.NoError(t, err)
	require.NoError(t, e.BulkCreate(context.Background(), testModelGenerator(10), opts))

	var last *Metrics
	for {
		select {
		case snapshot := <-metricsCh:
			last = snapshot
		default:
			require.NotNil(t, last, "BulkCreate must publish at least one metrics snapshot")
			require.Equal(t, StatusDone, last.Status, "the final metrics snapshot must report done")

			return
		}
	}
}

func TestRound2RefreshIndexShardFailures(t *testing.T) {
	t.Run("failed shard returns an error", func(t *testing.T) {
		srv := newRound2ESServer(t, round2ESConfig{
			refreshBody: `{"_shards":{"total":2,"successful":1,"failed":1}}`,
		})
		e := newTestEBI(t, srv)

		err := e.refreshIndex(context.Background(), "round2-refresh")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed")
	})

	t.Run("zero failed shards succeeds", func(t *testing.T) {
		srv := newRound2ESServer(t, round2ESConfig{
			refreshBody: `{"_shards":{"total":2,"successful":2,"failed":0}}`,
		})
		e := newTestEBI(t, srv)

		require.NoError(t, e.refreshIndex(context.Background(), "round2-refresh"))
	})
}

func TestRound2HyperparameterOptimization(t *testing.T) {
	minimalConfig := func() ho.OptimizationConfig {
		cfg := ho.DefaultConfig()
		cfg.InitialSamples = 1
		cfg.Iterations = 0
		cfg.NumCandidates = 1

		return cfg
	}

	ranges := []ho.ParameterRange[int]{
		{Min: 1, Max: 1},
		{Min: 1, Max: 1},
	}

	t.Run("fewer than two ranges returns an error", func(t *testing.T) {
		f := newCaptureES(t, captureESConfig{})
		e := newTestEBI(t, f.srv)
		opts, err := NewBulkOptions[*TestModel]("round2-hpo-ranges", rawMessage)
		require.NoError(t, err)

		var (
			best   []int
			hpoErr error
		)
		require.NotPanics(t, func() {
			best, hpoErr = e.HyperparameterOptimization(
				context.Background(),
				testModelGenerator(1),
				opts,
				minimalConfig(),
				ranges[:1],
			)
		})
		require.Error(t, hpoErr)
		require.Nil(t, best)
	})

	t.Run("all failed trials return an error", func(t *testing.T) {
		srv := newRound2ESServer(t, round2ESConfig{bulkStatus: http.StatusInternalServerError})
		e := newTestEBI(t, srv)
		opts, err := NewBulkOptions[*TestModel]("round2-hpo-failure", rawMessage)
		require.NoError(t, err)

		best, err := e.HyperparameterOptimization(
			context.Background(),
			testModelGenerator(1),
			opts,
			minimalConfig(),
			ranges,
		)
		require.Error(t, err)
		require.Len(t, best, 2)
	})

	t.Run("successful run preserves caller options", func(t *testing.T) {
		f := newCaptureES(t, captureESConfig{})
		e := newTestEBI(t, f.srv)
		opts, err := NewBulkOptions(
			"round2-hpo-success",
			rawMessage,
			WithBatchSize[*TestModel](7),
			WithDocumentIDFunc(func(doc *TestModel) string { return doc.ID }),
			WithNumWorkers[*TestModel](NumWorkersManual(3)),
		)
		require.NoError(t, err)

		type optionSnapshot struct {
			batchSize     int
			flushBytes    int
			index         string
			metricsCheck  time.Duration
			operation     string
			pauseDuration time.Duration
			workers       int
		}

		workersBefore, err := opts.NumWorkers()
		require.NoError(t, err)
		before := optionSnapshot{
			batchSize:     opts.BatchSize,
			flushBytes:    opts.FlushBytes,
			index:         opts.Index,
			metricsCheck:  opts.MetricsCheck,
			operation:     opts.Operation,
			pauseDuration: opts.PauseDuration,
			workers:       workersBefore,
		}

		best, err := e.HyperparameterOptimization(
			context.Background(),
			testModelGenerator(2),
			opts,
			minimalConfig(),
			ranges,
		)
		require.NoError(t, err)
		require.Equal(t, []int{1, 1}, best)

		workersAfter, err := opts.NumWorkers()
		require.NoError(t, err)
		after := optionSnapshot{
			batchSize:     opts.BatchSize,
			flushBytes:    opts.FlushBytes,
			index:         opts.Index,
			metricsCheck:  opts.MetricsCheck,
			operation:     opts.Operation,
			pauseDuration: opts.PauseDuration,
			workers:       workersAfter,
		}
		require.Equal(t, before, after, "HyperparameterOptimization must not mutate caller options")
	})
}
