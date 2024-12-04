# ebi (Elasticsearch Bulk Indexer)

![ebi](https://github.com/user-attachments/assets/97e359ae-3a76-48f8-b5db-9b3604bd4e6f)

A powerful Go package for efficient bulk indexing in Elasticsearch that leverages machine learning for hyperparameter optimization. The package eliminates guesswork in ETL and data loading by automatically finding optimal indexing parameters through Bayesian optimization, while providing automated, metrics-driven bulk indexing with built-in circuit breakers and monitoring capabilities.

## Why Hyperparameter Optimization?

ETL and data loading processes often involve significant trial and error to find the right balance between:
- Batch sizes that maximize throughput without overwhelming memory
- Worker counts that optimize parallelism without causing contention
- Buffer sizes that enhance performance without risking stability

Instead of manual tuning or using fixed "best practices" that may not suit your specific use case, ebi uses machine learning to:
1. Automatically discover optimal parameters for your unique data characteristics
2. Adapt to different document sizes and complexity
3. Account for your cluster's specific hardware capabilities
4. Balance indexing speed with resource utilization
5. Reduce operational overhead and time-to-production

The integrated Hyperparameter Optimization (HO) uses Bayesian optimization with Gaussian Processes to:
- Efficiently explore the parameter space
- Learn from previous indexing attempts
- Converge on optimal values faster than grid or random search
- Adapt to changes in data patterns or cluster conditions

## Features

- **Machine Learning-Driven Optimization**: 
  - Automated discovery of optimal indexing parameters
  - Bayesian optimization with Gaussian Processes
  - Learning from historical performance
  - Continuous adaptation to changing conditions
  - Optimization objectives:
    - Batch Size
    - Number of Workers
    - Buffer Allocations
    - Network Utilization

- **Smart Bulk Indexing**: 
  - Automatic parameter calculation based on document characteristics
  - Dynamic adjustment to cluster capabilities
  - Resource-aware scaling
  - Efficient memory management

- **Real-time Metrics**: Comprehensive monitoring of bulk operations and index health
- **Circuit Breakers**: Built-in protection against cluster overload through configurable pause conditions
- **Progress Monitoring**: Real-time updates via channels for:
  - Bulk Operation Metrics
  - Error Reporting
- **Generic Implementation**: Works with any document type via Go generics
- **Flexible Configuration**: 
  - **Core Parameters**:
    - Automatically tuned batch size and worker count
    - Configurable flush intervals and buffer sizes
    - Custom refresh policies (immediate, wait_for, false)
    - Automatic retries on failure
  - **Dynamic Functions**:
    - Custom document ID generation
    - Dynamic index name modification
    - Routing key calculation
    - Conditional pausing based on metrics
    - Smart index refresh triggers
  - **Operational Controls**:
    - Configurable metrics check intervals
    - Adjustable pause durations
    - Customizable error handling
- **Robust Error Handling**: Comprehensive error catalog with detailed failure reporting

## Install

```bash
go get github.com/thalesfsp/ebi
```

## Metrics

### Bulk Operation Metrics
- Documents Processed/Failed/Succeeded
- Bytes Processed
- Error Count

### Index Health Metrics
- RAM per Node
- Number of Data Nodes
- JVM Heap Usage
- Status (Running/Paused)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request
