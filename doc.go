/*
Package ebi provides a powerful and efficient bulk indexing solution for Elasticsearch with
intelligent hyperparameter optimization. It offers automated, metrics-driven bulk indexing
with built-in circuit breakers, progress monitoring, and self-tuning capabilities.

# Smart Bulk Indexing

The package automatically calculates optimal batch sizes and worker counts based on document
size and cluster configuration. It uses Bayesian optimization via the integrated 'ho' package
to fine-tune operational parameters such as batch size and number of workers.

	indexer := ebi.New[MyDocType](client, indexName)
	indexer.Start(ctx, documents)

# Real-time Metrics

Comprehensive monitoring is provided for bulk operations, index health, and cluster status.
The package tracks various metrics including:

Bulk Operation Metrics:
  - Documents Processed/Failed/Succeeded
  - Bytes Processed
  - Error Count and Details
  - Failed Items List

Index Health Metrics:
  - Circuit Breaker States
  - JVM Heap Usage
  - Indexing Pressure
  - Segments Count and Memory
  - Translog Size
  - Unassigned Shards

# Circuit Breakers

Built-in protection mechanisms prevent cluster overload by monitoring:
  - JVM Heap Usage
  - Circuit Breaker States
  - Indexing Pressure
  - Translog Size

# Progress Monitoring

Real-time updates are available via channels for:
  - Bulk Operation Metrics
  - Index Health Metrics
  - Error Reporting

# Generic Implementation

The package uses Go generics to work with any document type:

	type MyDocument struct {
		ID   string `json:"id"`
		Data string `json:"data"`
	}

	indexer := ebi.New[MyDocument](client, indexName)

# Flexible Configuration

The indexing process can be customized with various options:

	indexer := ebi.New[MyDocument](client, indexName,
		ebi.WithBatchSize(1000),
		ebi.WithWorkers(4),
		ebi.WithOptimization(true),
	)

# Error Handling

The package provides comprehensive error handling with detailed failure reporting:

	if err := indexer.Start(ctx, documents); err != nil {
		var bulkErr *ebi.BulkError
		if errors.As(err, &bulkErr) {
			// Handle bulk indexing error
		}
	}

For more information and examples, visit: https://github.com/thalesfsp/ebi
*/
package ebi
