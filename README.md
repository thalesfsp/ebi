# ebi (Elasticsearch Bulk Indexer)

![ebi](https://github.com/user-attachments/assets/97e359ae-3a76-48f8-b5db-9b3604bd4e6f)

A powerful Go package for efficient bulk indexing in Elasticsearch that leverages machine learning for hyperparameter optimization. The package eliminates guesswork in ETL and data loading by automatically finding optimal indexing parameters through Bayesian optimization, while providing automated, metrics-driven bulk indexing with built-in circuit breakers, real-time monitoring, and intelligent resource management.

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
  - Automated discovery of optimal indexing parameters using Bayesian optimization
  - Gaussian Processes with Upper Confidence Bound (UCB) acquisition function
  - Learning from historical performance data
  - Continuous adaptation to changing data patterns and cluster conditions
  - Optimization targets:
    - Batch Size (optimal document grouping)
    - Number of Workers (parallel processing optimization)
    - Buffer Allocations (memory usage efficiency)
    - Network Utilization (throughput maximization)

- **Intelligent Bulk Indexing**: 
  - **BulkCreate**: High-performance bulk indexing with automatic parameter tuning
  - **Multi-Operation Support**: Index, Create, Update (with upsert), Delete operations
  - **Dynamic Configuration**: Runtime adjustment based on document characteristics
  - **Cluster-Aware Scaling**: Automatic discovery and utilization of data nodes
  - **Resource-Aware Processing**: JVM heap-based worker calculation
  - **Smart Memory Management**: Document size-based batch optimization

- **Real-time Monitoring & Metrics**: 
  - **Live Progress Tracking**: Real-time updates via Go channels
  - **Comprehensive Metrics**: Documents processed/failed/succeeded, bytes processed, error counts
  - **Cluster Health Monitoring**: RAM per node, JVM heap usage, data node discovery
  - **Performance Analytics**: Throughput analysis and bottleneck identification

- **Advanced Circuit Breakers**: 
  - **Intelligent Pausing**: Configurable conditions based on cluster pressure metrics
  - **Automatic Recovery**: Smart resume when conditions improve
  - **Custom Pause Logic**: User-defined pause functions based on any metrics
  - **Graceful Degradation**: Continue processing healthy documents when some fail

- **Flexible Configuration System**: 
  - **Core Parameters**:
    - Automatically calculated batch size and worker count
    - Configurable flush intervals and buffer sizes (`BatchSize` drives the flush size whenever `FlushBytes` isn't set)
    - Multiple refresh policies (immediate, wait_for, false)
    - `RetryOnFailure` is reserved for future use — it is validated but not currently wired into indexing behavior
  - **Dynamic Functions**:
    - Custom document ID generation (`DocumentIDFunc`)
    - Dynamic index name modification (`IndexNameFunc`)
    - Routing key calculation (`RoutingFunc`)
    - Conditional pausing logic (`PauseFunc`)
    - Smart index refresh triggers (`RefreshFunc`)
  - **Operational Controls**:
    - Configurable metrics check intervals
    - Adjustable pause durations
    - Comprehensive error handling via channels
    - Thread-safe status management

- **Production-Ready Features**:
  - **Generic Implementation**: Works with any document type via Go generics
  - **Robust Error Handling**: Comprehensive error catalog with detailed failure reporting
  - **Async Error Processing**: Non-blocking error handling via channels
  - **Context Support**: Full context propagation for cancellation and timeouts
  - **Connection Management**: Automatic connection testing and health verification

## Core Operations

### Supported Elasticsearch Operations

EBI supports all major Elasticsearch bulk operations:

- **Index**: Add new documents or replace existing ones
- **Create**: Add new documents (fails if document already exists)
- **Update**: Modify existing documents with partial updates
  - Support for `doc_as_upsert` (create if document doesn't exist)
  - Automatic document wrapping in `"doc"` field for update operations
- **Delete**: Remove documents (no body required)

### BulkCreate Method

The main indexing method with comprehensive configuration options.

**Key Features:**
- Automatic parameter optimization based on document size and cluster capacity
- Real-time metrics monitoring with configurable intervals
- Intelligent pause/resume based on cluster pressure
- Async error handling via channels
- Support for custom document ID generation and routing
- Dynamic index name modification
- Graceful handling of individual document failures

### HyperparameterOptimization Method

ML-driven optimization for finding optimal bulk indexing parameters:

**Optimization Process:**
- Uses Gaussian Process with Upper Confidence Bound (UCB) acquisition function
- Tests different combinations of batch size and worker count
- Learns from indexing performance to optimize future attempts
- Returns optimal parameters `[batchSize, numWorkers]`
- Minimizes indexing errors while maximizing throughput

## Requirements

- Go **1.25** or later (raised in v0.3.0 by the `customerror/v2` dependency).

## Install

```bash
go get github.com/thalesfsp/ebi
```

## Upgrading to v0.4.0

If you're upgrading from an older release, check these before you deploy:

- **Go 1.25+ is required** (raised in v0.3.0 by the `customerror/v2` dependency). Builds on older toolchains will fail.
- **`BulkCreate` now returns an error in cases that used to return `nil`**: documents that fail to marshal, flush-level failures (e.g. an HTTP 200 with an undecodable body), and responses that acknowledge fewer documents than were sent are all now surfaced. This is the headline benefit of the upgrade — silent data loss that previously returned success is now visible as an error.
- **`BatchSize` now drives flush cadence when `FlushBytes` is unset** (it was inert before v0.2.0). If you already set `BatchSize` and never set `FlushBytes`, your flush cadence may change on upgrade. Set `WithFlushBytes(...)` explicitly to pin the previous ~5MB-target behavior.
- **Error messages carry an `ERR_...: ` code prefix** (from `customerror/v2`, v0.3.0+). Replace any exact/full-string error matching with substring or error-code checks.
- See the [v0.2.0](https://github.com/thalesfsp/ebi/releases/tag/v0.2.0), [v0.3.0](https://github.com/thalesfsp/ebi/releases/tag/v0.3.0), and [v0.4.0](https://github.com/thalesfsp/ebi/releases/tag/v0.4.0) release notes for the full list of behavior changes.

## Quick Start

### Basic Usage

Create a simple document type and use EBI for basic bulk indexing:

1. Define your document structure using Go structs with JSON tags
2. Create an Elasticsearch client configuration with your cluster details
3. Initialize an EBI instance using the `New` factory function
4. Configure bulk options using `NewBulkOptions` with your index name and operation type
5. Call `BulkCreate` to perform the bulk indexing operation

The basic workflow involves creating bulk options, setting the target index, operation type (index/create/update/delete), and providing a sample document for size calculation.

### Advanced Usage with Monitoring

For production environments, enable comprehensive monitoring:

1. Create channels for real-time metrics and error reporting
2. Configure bulk options with monitoring channels enabled
3. Set up custom functions for document ID generation, routing, and conditional logic
4. Implement monitoring goroutines to track progress and handle errors
5. Use custom pause and refresh functions based on cluster metrics

Advanced features include custom document ID generation, dynamic index naming, routing calculation, intelligent pausing based on cluster pressure, and automatic index refresh triggers.

### Hyperparameter Optimization

Leverage machine learning to find optimal indexing parameters:

1. Prepare a representative sample of your data for optimization testing
2. Configure the optimization algorithm with iteration count and initial points
3. Define parameter ranges for batch size and worker count exploration
4. Run the hyperparameter optimization to find optimal settings
5. Apply the discovered optimal parameters to your production bulk options

The optimization process uses Bayesian optimization with Gaussian Processes to efficiently explore parameter combinations and learn from indexing performance.

## Metrics & Monitoring

EBI provides comprehensive real-time monitoring capabilities through structured metrics and channels.

### Available Metrics

#### Bulk Operation Metrics
- **DocsProcessed**: Total number of documents processed
- **DocsSucceeded**: Number of successfully indexed documents  
- **DocsFailed**: Number of documents that failed to index
- **BytesProcessed**: Total bytes processed during indexing
- **ErrorCount**: Total number of errors encountered
- **Status**: Current indexer status (`StatusRunning`, `StatusPaused`)

#### Cluster Health Metrics
- **RAMPerNode**: Available RAM per Elasticsearch node (GB)
- **NumDataNodes**: Number of data nodes in the cluster
- **JVMHeapUsage**: Current JVM heap utilization
- **MemoryPressure**: Overall memory pressure (0.0 to 1.0)
- **WritePressure**: Write operation pressure on the cluster
- **OverallPressure**: Combined system pressure metric
- **TranslogSize**: Current transaction log size (bytes)

### Real-time Monitoring

Monitor indexing progress and cluster health through structured metrics channels:

Set up monitoring channels for real-time metrics and error reporting, configure options with appropriate channel buffer sizes, and implement monitoring goroutines to track progress, status, throughput, and cluster health. The monitoring system provides comprehensive insights into document processing rates, error counts, memory pressure, and overall system performance.

> **Channel contract — read before wiring up `MetricsCh` and `ErrorCh`:**
> - Both channels are delivered **best-effort and non-blocking**. If a send can't complete immediately (buffer full or nobody reading), the snapshot or error is **dropped** — never queued, and never allowed to block an indexing worker. Buffer both channels generously if you need reliable delivery.
> - `MetricsCh` snapshots are point-in-time; the final snapshot sent before `BulkCreate` returns reports status `done`.
> - `ErrorCh` is detail, not your only failure signal: a failed operation is **always** reported through `BulkCreate`'s own returned error, even if the matching `ErrorCh` send was dropped.
> - **Never close `MetricsCh` or `ErrorCh`**, including after `BulkCreate` returns. When the caller's context is cancelled, the underlying bulk indexer does not wait for its workers to fully join, so a callback can fire briefly after `BulkCreate` returns — sending on a channel you've already closed panics the process. Just stop reading from it; there's nothing to clean up.

### Custom Monitoring Functions

#### Pause Function
Define custom conditions to pause indexing based on cluster metrics by implementing a function that receives metrics and returns a boolean. The function can check memory pressure thresholds, write pressure limits, error count boundaries, or any combination of cluster health indicators to determine when indexing should be paused.

#### Refresh Function
Control when to refresh the index based on specific conditions such as transaction log size, document count milestones, time intervals, or custom business logic. The refresh function receives context and metrics, allowing for intelligent refresh triggers that optimize both performance and data visibility.

## Configuration Options

### BulkOptions Configuration

The `BulkOptions` struct provides extensive configuration capabilities for customizing bulk indexing behavior:

**Core Configuration**: Set target index name, operation type (index/create/update/delete), batch size, worker count, flush thresholds, and sample documents for optimization calculations. When `FlushBytes` isn't set explicitly, `BatchSize` (multiplied by the sample document's size) determines the flush cadence; leave both unset to fall back to the ~5MB-target auto-calculation. `RetryOnFailure` is accepted and validated but currently reserved — it has no effect on indexing behavior yet.

**Update Operation Options**: Enable upsert behavior for update operations, allowing documents to be created if they don't exist.

**Dynamic Functions**: Configure custom document ID generation, dynamic index name modification, routing calculation, pause conditions, and refresh triggers.

**Monitoring & Error Handling**: Set up real-time metrics channels, error reporting channels, metrics check intervals, and pause durations.

### Worker Configuration

Configure the number of workers for optimal performance using several approaches:

- **Manual Configuration**: Set a specific number of workers based on your requirements
- **JVM-based Calculation**: Automatically calculate workers based on available JVM heap memory
- **Custom Functions**: Implement your own logic for determining optimal worker count based on cluster characteristics

### Refresh Policies

Control how and when Elasticsearch refreshes the index with three main policies:

- **No Refresh** (default): Fastest indexing with no forced refresh operations
- **Immediate Refresh**: Slowest indexing but immediate document visibility for searches  
- **Wait for Refresh**: Balanced approach waiting for natural refresh cycles

## Error Handling

EBI provides comprehensive error handling with detailed error reporting and graceful degradation capabilities.

> Error messages are built on `customerror/v2` and carry an `ERR_...: ` code prefix. Match on the error code or a substring, not the full message text, since the human-readable portion can change between releases.

### Error Categories

- **Configuration Errors**: Invalid bulk options, missing required fields
- **Connection Errors**: Elasticsearch connectivity issues, authentication failures
- **Indexing Errors**: Document-level failures, mapping conflicts, validation errors
- **Cluster Errors**: Resource exhaustion, node unavailability, timeout issues
- **Optimization Errors**: Hyperparameter optimization failures

### Async Error Handling

Errors are reported asynchronously via `ErrorCh`, allowing non-blocking operation. Delivery is **best-effort**: an error that can't be handed off immediately (channel full, or unbuffered with no reader ready) is **dropped**, not queued. Buffer `ErrorCh` generously if you want to observe every error, and never close it (see the channel contract above) — `BulkCreate`'s returned error is always the authoritative signal that an operation failed, independent of what arrives on `ErrorCh`.

### Graceful Degradation

EBI continues processing healthy documents even when some fail:

- Individual document failures don't stop the entire batch
- Detailed error reporting for each failed document
- Metrics tracking of success/failure rates
- Silent data loss is surfaced, not swallowed: marshal failures, flush-level failures, and under-acknowledged responses all now return an error from `BulkCreate` instead of a false `nil`

### Error Recovery

Built-in recovery mechanisms for common scenarios:

- **Connection Recovery**: Automatic reconnection on network issues
- **Pressure Relief**: Intelligent pausing when cluster is under pressure
- **Resource Management**: Dynamic adjustment of batch sizes and worker counts
- **Timeout Handling**: Proper context cancellation and cleanup

## Performance Tuning

### Automatic Optimization

EBI automatically optimizes performance based on:

1. **Document Size Analysis**: Calculates optimal batch sizes from sample documents
2. **Cluster Capacity**: Discovers data nodes and available resources
3. **JVM Heap Sizing**: Calculates worker count based on available memory
4. **Network Efficiency**: Optimizes buffer sizes for network utilization

### Manual Tuning

Fine-tune performance for specific use cases by adjusting parameters based on document characteristics and cluster capabilities:

**Large Documents**: Use smaller batch sizes and reduced flush byte thresholds to prevent memory overflow and maintain stable performance.

**Small Documents**: Increase batch sizes and flush byte limits to maximize throughput and reduce network overhead.

**High-Performance Clusters**: Increase worker count to fully utilize available cluster resources and processing power.

**Resource-Constrained Environments**: Reduce worker count to prevent resource contention and maintain system stability.

### Performance Monitoring

Track performance metrics to identify bottlenecks by implementing periodic monitoring with configurable intervals. Monitor documents per second, megabytes processed per second, and track changes in processing rates over time. Use ticker-based monitoring to sample metrics at regular intervals and calculate throughput rates for performance analysis.

## Best Practices

### 1. Use Hyperparameter Optimization

Start with hyperparameter optimization to find optimal settings by using a representative sample of your data for testing. Run the optimization process to discover the best combination of batch size and worker count parameters, then apply these optimal settings to your production indexing operations.

### 2. Monitor Cluster Health

Always monitor cluster metrics to prevent overload by implementing pause functions that check memory pressure and write pressure thresholds. Use the built-in metrics to make intelligent decisions about when to pause indexing operations.

### 3. Handle Errors Gracefully

Implement proper error handling for production use by setting up error channels with adequate buffer sizes — remember that `ErrorCh` delivery is best-effort and can drop errors under load, so always check `BulkCreate`'s returned error too. Monitor error rates and implement alerting mechanisms for high error conditions to ensure operational awareness.

### 4. Use Appropriate Refresh Policies

Choose refresh policy based on your use case requirements:

- **Real-time search**: Use immediate refresh policy (slower indexing but immediate visibility)
- **Near real-time**: Use wait-for refresh policy (balanced performance and visibility)  
- **Batch processing**: Use no refresh policy (fastest indexing with delayed visibility)

### 5. Optimize Document Structure

Structure your documents for efficient indexing by keeping nested objects minimal and using appropriate field types. Consider document size impact on batch calculations and memory usage.

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Install dependencies: `go mod download`
4. Run tests: `go test ./...`
5. Submit a pull request

### Running Tests

Run the complete test suite using standard Go testing commands:

- **All tests**: Use `go test ./...` to run all tests in the project
- **Coverage analysis**: Use `go test -cover ./...` to run tests with coverage reporting
- **Integration tests**: Use `go test -tags=integration ./...` to run integration tests (requires Elasticsearch)

