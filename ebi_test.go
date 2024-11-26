package ebi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/thalesfsp/ho"
)

//////
// Const, vars, and types.
//////

var (
	apiKey        = os.Getenv("ELASTICSEARCH_API_KEY")
	cloudID       = os.Getenv("ELASTICSEARCH_CLOUD_ID")
	baseIndexName = os.Getenv("ELASTICSEARCH_BASE_INDEX_NAME")

	rawMessage = json.RawMessage(`{"id": "1", "group": 1}`)
)

// TestModel is a test model.
type TestModel struct {
	ID    string `json:"id"`
	Group int    `json:"group"`
}

//////
// Helper function(s).
//////

// Test model generator.
func testModelGenerator(amount int) []*TestModel {
	acc := make([]*TestModel, 0, amount)

	for i := 0; i < amount; i++ {
		acc = append(acc, &TestModel{
			ID:    fmt.Sprintf("%d", i),
			Group: 1,
		})
	}

	return acc
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

	opts, err := NewBulkOptions(
		// Base index name. Will be modified by the indexNameFunc.
		baseIndexName,

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		rawMessage,

		RefreshPolicyFalse,

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

		nil, nil,
	)
	assert.NoError(t, err)

	// Not needed, only for testing purposes.
	opts.MetricsCheck = 100 * time.Millisecond

	metricsCh := make(chan *Metrics)
	defer close(metricsCh)

	errorCh := make(chan error)
	defer close(errorCh)

	// Go routine to do something with metrics and errors.
	go func() {
		// Receives metrics and print it.
		for {
			select {
			// REMEMBER, you MUST check `ok` value to avoid deadlock!
			case gm, ok := <-metricsCh:
				if !ok {
					return
				}

				t.Logf("Metrics: %+v\n", gm)

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
		metricsCh,

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

	opts, err := NewBulkOptions[*TestModel](
		// Base index name. Will be modified by the indexNameFunc.
		fmt.Sprintf("%s-test", baseIndexName),

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		rawMessage,

		RefreshPolicyFalse,

		nil, nil, nil, nil, nil,
	)
	assert.NoError(t, err)

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

	opts, err := NewBulkOptions[*TestModel](
		// Base index name. Will be modified by the indexNameFunc.
		fmt.Sprintf("%s-test-ho", baseIndexName),

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		rawMessage,

		RefreshPolicyFalse,

		nil, nil, nil, nil, nil,
	)
	assert.NoError(t, err)

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
		// Receives metrics and print it.
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

	opts, err := NewBulkOptions[*TestModel](
		// Base index name. Will be modified by the indexNameFunc.
		fmt.Sprintf("%s-test-ho", baseIndexName),

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		rawMessage,

		RefreshPolicyFalse,

		nil, nil, nil, nil, nil,
	)
	assert.NoError(t, err)

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
	assert.NoError(t, err)

	metrics := Metrics{}

	assert.NoError(t, updateIndexMetrics(
		context.Background(),
		e.client,
		&metrics,
	))

	b, err := json.MarshalIndent(metrics.GetMetrics(), "", "  ")
	assert.NoError(t, err)
	assert.NotEmpty(t, string(b))
}

func TestBulkCreate_Channels(t *testing.T) {
	t.Skip()

	// Create test context with timeout
	ctx := context.Background()

	// Initialize channels with buffer to prevent blocking.
	metricsChannel := make(chan *Metrics, 100)
	errorChannel := make(chan error, 100)

	// Create done channel for graceful shutdown.
	done := make(chan bool)

	// Start goroutine to collect metrics.
	go func() {
		defer close(done)

		for {
			select {
			case <-ctx.Done():
				return
			case metric, ok := <-metricsChannel:
				if !ok {
					return
				}

				// Convert metric to pretty JSON and print that.
				b, err := json.MarshalIndent(metric, "", "  ")
				assert.NoError(t, err)

				assert.NotEmpty(t, string(b))
			case err, ok := <-errorChannel:
				if !ok {
					return
				}

				t.Logf("Error: %v", err)

				t.Fail()
			}
		}
	}()

	// Initialize EBI client with test configuration.
	ebi, err := New[*TestModel](
		ctx,
		elasticsearch.Config{
			APIKey:  apiKey,
			CloudID: cloudID,
		},
	)

	assert.NotNil(t, ebi)
	assert.NoError(t, err)

	// Create test data
	testDocs := testModelGenerator(50_000)

	// Create bulk options
	opts, err := NewBulkOptions[*TestModel](
		baseIndexName,
		rawMessage,
		RefreshPolicyFalse,
		nil, nil, nil, nil, nil,
	)
	assert.NoError(t, err)

	opts.BatchSize = 500
	opts.NumWorkers = 1
	opts.MetricsCheck = 300 * time.Millisecond
	opts.FlushInterval = 1 * time.Second
	opts.RetryOnFailure = 1

	assert.NoError(t, ebi.BulkCreate(ctx, testDocs, opts, metricsChannel, errorChannel))

	// Will wait for goroutine to finish.
	<-done
}
