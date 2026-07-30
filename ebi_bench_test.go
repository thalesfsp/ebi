package ebi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
)

var (
	benchmarkBytesSink   []byte
	benchmarkMetricsSink *Metrics
	benchmarkStatusSink  Status
)

func BenchmarkBestEffortMetricsSend_FullBuffer(b *testing.B) {
	b.ReportAllocs()

	metrics, err := NewMetrics()
	if err != nil {
		b.Fatal(err)
	}

	metricsCh := make(chan *Metrics, 1)
	metricsCh <- metrics

	b.ResetTimer()

	for range b.N {
		bestEffortMetricsSend(metricsCh, metrics)
	}
}

func BenchmarkBestEffortMetricsSend_WithRoom(b *testing.B) {
	b.ReportAllocs()

	metrics, err := NewMetrics()
	if err != nil {
		b.Fatal(err)
	}

	metricsCh := make(chan *Metrics, 1)

	b.ResetTimer()

	for range b.N {
		bestEffortMetricsSend(metricsCh, metrics)
		benchmarkMetricsSink = <-metricsCh
	}
}

func BenchmarkPauseCheck(b *testing.B) {
	metrics, err := NewMetrics()
	if err != nil {
		b.Fatal(err)
	}

	metrics.UpdateStatus(StatusPaused)

	b.Run("NarrowAccessor", func(b *testing.B) {
		b.ReportAllocs()

		for range b.N {
			benchmarkStatusSink = metrics.getStatus()
		}
	})

	b.Run("FullSnapshot", func(b *testing.B) {
		b.ReportAllocs()

		for range b.N {
			benchmarkStatusSink = metrics.GetMetrics().Status
		}
	})
}

func BenchmarkAddProcessed(b *testing.B) {
	b.Run("SingleLock", func(b *testing.B) {
		b.ReportAllocs()

		metrics, err := NewMetrics()
		if err != nil {
			b.Fatal(err)
		}

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				metrics.AddProcessed(128)
			}
		})
	})

	b.Run("TwoCallPair", func(b *testing.B) {
		b.ReportAllocs()

		metrics, err := NewMetrics()
		if err != nil {
			b.Fatal(err)
		}

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				metrics.IncreaseDocsProcessed()
				metrics.UpdateBytesProcessed(128)
			}
		})
	})
}

func BenchmarkUpdateEnvelopeMarshal(b *testing.B) {
	doc := &TestModel{ID: "42", Group: 3, Something: "identical bytes"}
	typedEnvelope := testUpdateEnvelope[*TestModel]{
		Doc:         doc,
		DocAsUpsert: true,
	}
	mapEnvelope := map[string]any{
		"doc":           doc,
		"doc_as_upsert": true,
	}

	typedBytes, err := json.Marshal(typedEnvelope)
	if err != nil {
		b.Fatal(err)
	}

	mapBytes, err := json.Marshal(mapEnvelope)
	if err != nil {
		b.Fatal(err)
	}

	if !bytes.Equal(typedBytes, mapBytes) {
		b.Fatalf("typed and map envelopes differ:\ntyped: %s\nmap:   %s", typedBytes, mapBytes)
	}

	b.Run("TypedEnvelope", func(b *testing.B) {
		b.ReportAllocs()

		for range b.N {
			benchmarkBytesSink, _ = json.Marshal(testUpdateEnvelope[*TestModel]{
				Doc:         doc,
				DocAsUpsert: true,
			})
		}
	})

	b.Run("MapBaseline", func(b *testing.B) {
		b.ReportAllocs()

		for range b.N {
			benchmarkBytesSink, _ = json.Marshal(map[string]any{
				"doc":           doc,
				"doc_as_upsert": true,
			})
		}
	})
}

func BenchmarkBulkCreate_FakeES_1kDocs(b *testing.B) {
	b.ReportAllocs()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")

		switch {
		case r.URL.Path == "/" || r.URL.Path == "":
			if r.Method != http.MethodHead {
				_, _ = w.Write([]byte(`{
					"name":"benchmark-node",
					"cluster_name":"benchmark-cluster",
					"cluster_uuid":"benchmark-uuid",
					"version":{"number":"8.11.0","build_flavor":"default","build_type":"docker",
						"build_hash":"abc","build_date":"2024-01-01","build_snapshot":false,
						"lucene_version":"9.0.0","minimum_wire_compatibility_version":"7.17.0",
						"minimum_index_compatibility_version":"7.0.0"},
					"tagline":"You Know, for Search"
				}`))
			}
		case strings.Contains(r.URL.Path, "_nodes/stats"):
			_, _ = w.Write([]byte(`{
				"_nodes":{"total":1,"successful":1,"failed":0},
				"cluster_name":"benchmark-cluster",
				"nodes":{}
			}`))
		case strings.Contains(r.URL.Path, "_bulk"):
			_, _ = w.Write(bulkSuccessResponse(r.Body))
		case strings.HasSuffix(r.URL.Path, "/_refresh"):
			_, _ = w.Write([]byte(`{"_shards":{"total":1,"successful":1,"failed":0}}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	b.Cleanup(srv.Close)

	e, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{Addresses: []string{srv.URL}},
	)
	if err != nil {
		b.Fatal(err)
	}

	metricsCh := make(chan *Metrics, 4096)
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)

		for metrics := range metricsCh {
			benchmarkMetricsSink = metrics
		}
	}()

	b.Cleanup(func() {
		close(metricsCh)
		<-readerDone
	})

	docs := testModelGenerator(1000)
	opts, err := NewBulkOptions(
		"benchmark-index",
		rawMessage,
		WithMetricsCh[*TestModel](metricsCh),
		WithDocumentIDFunc(func(doc *TestModel) string { return doc.ID }),
		WithNumWorkers[*TestModel](NumWorkersManual(1)),
	)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for range b.N {
		if err := e.BulkCreate(context.Background(), docs, opts); err != nil {
			b.Fatal(err)
		}
	}
}
