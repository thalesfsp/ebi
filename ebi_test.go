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
	ID        string `json:"id"`
	Group     int    `json:"group"`
	Something string `json:"something"`
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

	// Create test context with timeout
	ctx := context.Background()

	// Initialize channels with buffer to prevent blocking.
	metricsCh := make(chan *Metrics, 2)
	errorCh := make(chan error, 2)

	// Start goroutine to collect metrics.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case metric, ok := <-metricsCh:
				if !ok {
					return
				}

				// Convert metric to pretty JSON and print that.
				b, err := json.MarshalIndent(metric, "", "  ")
				assert.NoError(t, err)

				assert.NotEmpty(t, string(b))
			case err, ok := <-errorCh:
				if !ok {
					return
				}

				t.Logf("Error: %v", err)

				t.Fail()
			}
		}
	}()

	// Ideally use a pointer to the model.
	ebi, err := New[*TestModel](
		ctx,
		elasticsearch.Config{
			Addresses: []string{"http://localhost:9200"},
		},
	)
	assert.NotNil(t, ebi)
	assert.NoError(t, err)

	// Create test data
	testDocs := testModelGenerator(100)

	// Create bulk options
	opts, err := NewBulkOptions(
		// Base index name. Will be modified by the indexNameFunc.
		baseIndexName,

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		rawMessage,

		// Dynamically modify the index name, not needed but recommended.
		WithIndexNameFunc[*TestModel](func(indexName string) string {
			if indexName != "" {
				return fmt.Sprintf("%s-%d", indexName, time.Now().Unix())
			}

			return ""
		}),

		// Dynamically modify the document ID, not needed but recommended.
		WithDocumentIDFunc(func(doc *TestModel) string {
			if doc != nil {
				return fmt.Sprintf("%s-%d", doc.ID, time.Now().Unix())
			}

			return ""
		}),

		// Dynamically set routing, optional.
		WithRoutingFunc(func(doc *TestModel) string {
			if doc != nil {
				return fmt.Sprintf("%d", doc.Group)
			}

			return ""
		}),

		// Optional, but set here for demonstration purposes.
		WithMetricsCh[*TestModel](metricsCh),

		// Optional, but set here for demonstration purposes.
		WithErrorCh[*TestModel](errorCh),

		// Optional, but set here for demonstration purposes.
		WithBatchSize[*TestModel](500),

		// Optional, but set here for demonstration purposes.
		WithNumWorkers[*TestModel](NumWorkersAutoDiscovery(ctx, ebi)),

		// Optional, but set here for demonstration purposes.
		WithMetricsCheck[*TestModel](300*time.Millisecond),

		// Optional, but set here for demonstration purposes.
		WithFlushInterval[*TestModel](1*time.Second),

		// Optional, but set here for demonstration purposes.
		WithRetryOnFailure[*TestModel](1),
	)
	assert.NoError(t, err)

	assert.NoError(t, ebi.BulkCreate(ctx, testDocs, opts))

	close(metricsCh)
	close(errorCh)
}

func TestBulkCreate(t *testing.T) {
	t.Skip()

	// Ideally use a pointer to the model.
	ebi, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{
			Addresses: []string{"http://localhost:9200"},
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
	); err != nil {
		t.Fatal(err)
	}
}

func TestHO_WithChannel(t *testing.T) {
	t.Skip()

	ctx := context.Background()

	//////
	// ES client setup.
	//////

	// Ideally use a pointer to the model.
	ebi, err := New[*TestModel](
		ctx,
		elasticsearch.Config{
			Addresses: []string{"http://localhost:9200"},
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
		ctx,
		testModelGenerator(50_000),
		opts,
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
			Addresses: []string{"http://localhost:9200"},
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
	)
	assert.NoError(t, err)

	//////
	// HO itself.
	//////

	bestParams, err := ebi.HyperparameterOptimization(
		context.Background(),
		testModelGenerator(50_000),
		opts,
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
			Addresses: []string{"http://localhost:9200"},
		},
	)
	assert.NoError(t, err)

	metrics := Metrics{}

	assert.NoError(t, updateIndexMetrics(
		context.Background(),
		e,
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

	// Start goroutine to collect metrics.
	go func() {
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
			Addresses: []string{"http://localhost:9200"},
		},
	)

	assert.NotNil(t, ebi)
	assert.NoError(t, err)

	// Create test data
	testDocs := testModelGenerator(50_000)

	// Create bulk options
	opts, err := NewBulkOptions(
		baseIndexName,
		rawMessage,
		WithMetricsCh[*TestModel](metricsChannel),
		WithErrorCh[*TestModel](errorChannel),
		WithNumWorkers[*TestModel](NumWorkersAutoDiscovery(ctx, ebi)),
	)
	assert.NoError(t, err)

	opts.BatchSize = 500
	opts.NumWorkers = NumWorkersManual(1)
	opts.MetricsCheck = 300 * time.Millisecond
	opts.FlushInterval = 1 * time.Second
	opts.RetryOnFailure = 1

	assert.NoError(t, ebi.BulkCreate(ctx, testDocs, opts))
}

func TestDiscoverWorkers(t *testing.T) {
	t.Skip()

	// Create test context with timeout
	ctx := context.Background()

	// Initialize EBI client with test configuration.
	ebi, err := New[*TestModel](
		ctx,
		elasticsearch.Config{
			Addresses: []string{"http://localhost:9200"},
		},
	)

	assert.NotNil(t, ebi)
	assert.NoError(t, err)

	n, err := ebi.discoverWorkerNodes(context.Background())
	assert.NoError(t, err)
	assert.NotZero(t, n)
}

func TestBulkCreate_indexUpdateDelete(t *testing.T) {
	t.Skip()

	// Ideally use a pointer to the model.
	ebi, err := New[*TestModel](
		context.Background(),
		elasticsearch.Config{
			APIKey:  apiKey,
			CloudID: cloudID,
		},
	)

	assert.NoError(t, err)
	assert.NotNil(t, ebi)

	indexName := fmt.Sprintf("%s-e2e", baseIndexName)

	// Defer cleanup: bulk delete the created documents.
	defer func() {
		// Create delete options.
		deleteOpts, err := NewBulkOptions(
			indexName,
			rawMessage,
			WithDocumentIDFunc(func(doc *TestModel) string {
				if doc != nil {
					return doc.ID
				}

				return ""
			}),

			WithOperation[*TestModel]("delete"),
		)

		assert.NoError(t, err)
		assert.NotNil(t, deleteOpts)

		// Documents to delete (only ID is needed for deletion).
		docsToDelete := []*TestModel{
			{ID: "1"},
			{ID: "2"},
		}

		// Perform bulk delete.
		assert.NoError(
			t,
			ebi.BulkCreate(context.Background(), docsToDelete, deleteOpts),
		)
	}()

	// Step 1: First insert the documents.
	insertOpts, err := NewBulkOptions(
		// Base index name.
		indexName,

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		rawMessage,

		// Sets ID, without this, the ID will be generated by Elasticsearch.
		WithDocumentIDFunc(func(doc *TestModel) string {
			if doc != nil {
				return doc.ID
			}

			return ""
		}),

		// Index is the default operation. Explicitly set it for clarity.
		WithOperation[*TestModel]("index"),
	)

	assert.NoError(t, err)
	assert.NotNil(t, insertOpts)

	// Insert initial documents.
	initialDocs := []*TestModel{
		{
			ID:        "1",
			Group:     1,       // Initial value.
			Something: "test1", // Additional field for testing.
		},
		{
			ID:        "2",
			Group:     100,     // Initial value.
			Something: "test2", // Additional field for testing.
		},
	}

	assert.NoError(
		t,
		ebi.BulkCreate(context.Background(), initialDocs, insertOpts),
	)

	// Step 2: Now update the documents.
	// Use map[string]interface{} for partial updates to avoid zero values
	updateOptsPartial, err := NewBulkOptions(
		// Same index name.
		indexName,

		// Sample of the document to be indexed. This will be used to calculate
		// bulk settings.
		rawMessage,

		// Sets ID, without this, the ID will be generated by Elasticsearch.
		WithDocumentIDFunc(func(doc map[string]interface{}) string {
			if doc != nil {
				if id, ok := doc["id"].(string); ok {
					return id
				}
			}

			return ""
		}),

		// Use update operation.
		WithOperation[map[string]interface{}]("update"),

		// Disable upsert for true update behavior. False is the default value,
		// but explicitly setting it for clarity.
		WithDocAsUpsert[map[string]interface{}](false),
	)

	assert.NoError(t, err)
	assert.NotNil(t, updateOptsPartial)

	// Update the documents with new values.
	// Use map[string]interface{} for true partial updates to avoid
	// setting zero values for omitted fields.
	updatedDocs := []map[string]interface{}{
		{
			"id":    "1",
			"group": 3, // Updated value - only this field will be updated.
		},
		{
			"id":    "2",
			"group": 7, // Updated value - only this field will be updated.
		},
	}

	// Create a new EBI instance for map[string]interface{} type
	ebiMap, err := New[map[string]interface{}](
		context.Background(),
		elasticsearch.Config{
			APIKey:  apiKey,
			CloudID: cloudID,
		},
	)

	assert.NoError(t, err)
	assert.NotNil(t, ebiMap)

	assert.NoError(
		t, ebiMap.BulkCreate(context.Background(), updatedDocs, updateOptsPartial),
	)
}

//nolint:forcetypeassert
func TestUpdateDocumentFormatting(t *testing.T) {
	// Create a test document.
	doc := &TestModel{
		ID:    "test-id",
		Group: 42,
	}

	// Test with update operation and upsert enabled (default).
	opts := &BulkOptions[*TestModel]{
		Operation:   "update",
		DocAsUpsert: true, // Default to true.
	}

	// Simulate the document formatting logic from BulkCreate.
	var data []byte
	var err error

	if opts.Operation == "update" {
		updateBody := map[string]interface{}{
			"doc": doc,
		}

		if opts.DocAsUpsert {
			updateBody["doc_as_upsert"] = true
		}

		data, err = json.Marshal(updateBody)
	} else {
		data, err = json.Marshal(doc)
	}

	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify the structure.
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	assert.NoError(t, err)

	// Check that doc field exists.
	docField, exists := result["doc"]
	assert.True(t, exists, "update operation should have 'doc' field")

	// Check that doc_as_upsert is set.
	upsertField, exists := result["doc_as_upsert"]
	assert.True(t, exists, "update operation should have 'doc_as_upsert' field")
	assert.True(t, upsertField.(bool), "doc_as_upsert should be true")

	// Verify the document content.
	docMap := docField.(map[string]interface{})
	assert.Equal(t, "test-id", docMap["id"])
	assert.Equal(t, float64(42), docMap["group"]) // JSON unmarshaling converts numbers to float64.

	// Test with upsert disabled.
	opts.DocAsUpsert = false

	updateBody2 := map[string]interface{}{
		"doc": doc,
	}

	if opts.DocAsUpsert {
		updateBody2["doc_as_upsert"] = true
	}

	data, err = json.Marshal(updateBody2)
	assert.NoError(t, err)

	var result2 map[string]interface{}
	err = json.Unmarshal(data, &result2)
	assert.NoError(t, err)

	// Should not have doc_as_upsert when disabled.
	_, exists = result2["doc_as_upsert"]
	assert.False(t, exists, "doc_as_upsert should not be present when disabled")
}
