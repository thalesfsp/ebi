# ebi (Elasticsearch Bulk Indexer)

A powerful Go package for efficient bulk indexing in Elasticsearch with intelligent hyperparameter optimization. The package provides automated, metrics-driven bulk indexing with built-in circuit breakers, progress monitoring, and self-tuning capabilities.

## Features

- **Smart Bulk Indexing**: Automatically calculates optimal batch sizes and worker counts based on document size and cluster configuration
- **Real-time Metrics**: Comprehensive monitoring of bulk operations, index health, and cluster status
- **Circuit Breakers**: Built-in protection against cluster overload:
  - JVM Heap Usage
  - Circuit Breaker States
  - Indexing Pressure
  - Translog Size
- **Hyperparameter Optimization**: Uses Bayesian optimization (via integrated `ho` package) to fine-tune:
  - Batch Size
  - Number of Workers
- **Progress Monitoring**: Real-time updates via channels for:
  - Bulk Operation Metrics
  - Index Health Metrics
  - Error Reporting
- **Generic Implementation**: Works with any document type via Go generics
- **Flexible Configuration**: Highly customizable indexing process with dynamic modifications
- **Robust Error Handling**: Comprehensive error catalog with detailed failure reporting

## Install

```bash
go get github.com/thalesfsp/ebi
```

## Metrics

### Bulk Metrics
- Documents Processed/Failed/Succeeded
- Bytes Processed
- Error Count and Details
- Failed Items List

### Index Metrics
- Circuit Breaker States
- JVM Heap Usage
- Indexing Pressure
- Segments Count and Memory
- Translog Size
- Unassigned Shards

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

