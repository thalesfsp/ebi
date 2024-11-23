package ebi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/thalesfsp/ho"
)

var (
	apiKey        = os.Getenv("ELASTICSEARCH_API_KEY")
	cloudID       = os.Getenv("ELASTICSEARCH_CLOUD_ID")
	baseIndexName = os.Getenv("ELASTICSEARCH_BASE_INDEX_NAME")
)

// TestModel is a test model.
type TestModel struct {
	ID    string `json:"id"`
	Group int    `json:"group"`
}

func TestBulkCreate_WithChannelsAndFunctions(t *testing.T) {
	t.Skip()

	// Ideally use a pointer to the model.
	ebi, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{
			APIKey:  apiKey,
			CloudID: cloudID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	opts := NewBulkOptions(
		// Base index name. Will be modified by the indexNameFunc.
		baseIndexName,

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		json.RawMessage(`{"id": "1", "group": 1}`),

		// Cluster settings: data nodes and memory per node.
		3, 4,

		// Should refresh after bulk create, ideally no.
		false,

		// Dynamically modify the index name, not needed but recommended.
		func(indexName string) string {
			return fmt.Sprintf("%s-%d", indexName, time.Now().Unix())
		},

		// Dynamically modify the document ID, not needed but recommended.
		func(doc *TestModel) string {
			time.Sleep(1 * time.Second)

			return fmt.Sprintf("%s-%d", doc.ID, time.Now().Unix())
		},

		// Dynamically set routing, optional.
		func(doc *TestModel) string {
			return fmt.Sprintf("%d", doc.Group)
		},
	)

	// Not needed, only for testing purposes.
	opts.MetricsCheck = 100 * time.Millisecond

	globalMetricsCh := make(chan *GlobalMetrics)
	defer close(globalMetricsCh)

	errorCh := make(chan error)
	defer close(errorCh)

	// Go routine to do something with metrics and errors.
	go func() {
		// Receives global metrics and print it.
		for {
			select {
			// REMEMBER, you MUST check `ok` value to avoid deadlock!
			case gm, ok := <-globalMetricsCh:
				if !ok {
					return
				}

				t.Logf("GlobalMetrics.BulkMetrics: %+v\n", gm.BulkMetrics)

				t.Logf("GlobalMetrics.IndexMetrics: %+v\n", gm.IndexMetrics)

			// REMEMBER, you MUST check `ok` value to avoid deadlock!
			case err, ok := <-errorCh:
				if !ok {
					return
				}

				t.Logf("Error: %s\n", err)
			}
		}
	}()

	if err := ebi.BulkCreate(
		context.Background(),
		[]*TestModel{
			{
				ID:    "1",
				Group: 1,
			},
			{
				ID:    "2",
				Group: 1,
			},
		},
		opts,

		// Optional, but set here for demonstration purposes.
		globalMetricsCh,

		// Optional, but set here for demonstration purposes.
		errorCh,
	); err != nil {
		t.Fatal(err)
	}
}

func TestBulkCreate(t *testing.T) {
	t.Skip()

	// Ideally use a pointer to the model.
	ebi, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{
			APIKey:  apiKey,
			CloudID: cloudID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	opts := NewBulkOptions[*TestModel](
		// Base index name. Will be modified by the indexNameFunc.
		fmt.Sprintf("%s-test", baseIndexName),

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		json.RawMessage(`{"id": "1", "group": 1}`),

		// Cluster settings: data nodes and memory per node.
		3, 4,

		// Should refresh after bulk create, ideally no.
		false,

		nil, nil, nil,
	)

	if err := ebi.BulkCreate(
		context.Background(),
		[]*TestModel{
			{
				ID:    "1",
				Group: 1,
			},
			{
				ID:    "2",
				Group: 1,
			},
		},
		opts,

		nil, nil,
	); err != nil {
		t.Fatal(err)
	}
}

func TestHO_WithChannel(t *testing.T) {
	t.Skip()

	//////
	// Helper function.
	//////

	// Test model generator.
	testModelGenerator := func(amount int) []*TestModel {
		t.Helper()

		acc := make([]*TestModel, 0, amount)

		for i := 0; i < amount; i++ {
			acc = append(acc, &TestModel{
				ID:    fmt.Sprintf("%d", i),
				Group: 1,
			})
		}

		return acc
	}

	//////
	// ES client setup.
	//////

	// Ideally use a pointer to the model.
	ebi, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{
			APIKey:  apiKey,
			CloudID: cloudID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	//////
	// Bulk setup.
	//////

	opts := NewBulkOptions[*TestModel](
		// Base index name. Will be modified by the indexNameFunc.
		fmt.Sprintf("%s-test-ho", baseIndexName),

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		json.RawMessage(`{"id": "1", "group": 1}`),

		// Cluster settings: data nodes and memory per node.
		3, 4,

		// Should refresh after bulk create, ideally no.
		false,

		nil, nil, nil,
	)

	//////
	// HO setup.
	//////

	// Using default configuration and UCB as acquisition.
	// function.
	optimizationConfig := ho.DefaultConfig()

	// More would be good, but we don't want to wait too long.
	optimizationConfig.Iterations = 25

	//////
	// HO observability.
	//////

	// Create a bidirectional channel for progress updates
	progressChan := make(chan ho.ProgressUpdate, optimizationConfig.InitialSamples+optimizationConfig.Iterations)
	defer close(progressChan)

	// Assign the channel to config (will be automatically converted to send-only)
	optimizationConfig.ProgressChan = progressChan

	// Start a goroutine to handle progress updates.
	// Go routine to do something with metrics and errors.
	go func() {
		// Receives global metrics and print it.
		for {
			select {
			// REMEMBER, you MUST check `ok` value to avoid deadlock!
			case progress, ok := <-progressChan:
				if !ok {
					return
				}

				t.Logf("ProgressUpdate: %+v\n", progress)
			}
		}
	}()

	//////
	// HO itself.
	//////

	bestParams, err := ebi.HyperparameterOptimization(
		context.Background(),
		opts,
		testModelGenerator(50_000),
		optimizationConfig,
		// Hyperparameter ranges.
		[]ho.ParameterRange[int]{
			// Batch size.
			{Min: 1000, Max: 10000},

			// Number of workers.
			{Min: 3, Max: 9},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Best parameters: %+v\n", bestParams)
}

func TestHO(t *testing.T) {
	t.Skip()

	//////
	// Helper function.
	//////

	// Test model generator.
	testModelGenerator := func(amount int) []*TestModel {
		t.Helper()

		acc := make([]*TestModel, 0, amount)

		for i := 0; i < amount; i++ {
			acc = append(acc, &TestModel{
				ID:    fmt.Sprintf("%d", i),
				Group: 1,
			})
		}

		return acc
	}

	//////
	// ES client setup.
	//////

	// Ideally use a pointer to the model.
	ebi, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{
			APIKey:  apiKey,
			CloudID: cloudID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	//////
	// Bulk setup.
	//////

	opts := NewBulkOptions[*TestModel](
		// Base index name. Will be modified by the indexNameFunc.
		fmt.Sprintf("%s-test-ho", baseIndexName),

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		json.RawMessage(`{"id": "1", "group": 1}`),

		// Cluster settings: data nodes and memory per node.
		3, 4,

		// Should refresh after bulk create, ideally no.
		false,

		nil, nil, nil,
	)

	//////
	// HO itself.
	//////

	bestParams, err := ebi.HyperparameterOptimization(
		context.Background(),
		opts,
		testModelGenerator(50_000),
		ho.DefaultConfig(),
		// Hyperparameter ranges.
		[]ho.ParameterRange[int]{
			// Batch size.
			{Min: 1000, Max: 10000},

			// Number of workers.
			{Min: 3, Max: 9},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Best parameters: %+v\n", bestParams)
}

func TestUpdateIndexMetrics(t *testing.T) {
	t.Skip()

	// Ideally use a pointer to the model.
	e, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{
			APIKey:  apiKey,
			CloudID: cloudID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	metrics := IndexMetrics{}
	metrics.CircuitBreakers = make(map[string]float64)

	if err := updateIndexMetrics(
		context.Background(),
		baseIndexName,
		e.client,
		&metrics,
	); err != nil {
		t.Fatal(err)
	}

	t.Logf("IndexMetrics: %+v\n", metrics)
}
