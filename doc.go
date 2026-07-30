/*
Package ebi provides a powerful and efficient bulk indexing solution for Elasticsearch with
intelligent hyperparameter optimization. It offers automated, metrics-driven bulk indexing
with built-in circuit breakers, progress monitoring, and self-tuning capabilities.

# Requirements

Go 1.25 or later.

# Channel contracts

BulkCreate delivers on MetricsCh and ErrorCh best-effort and non-blocking: a snapshot or
error that cannot be handed off immediately is dropped rather than queued, so buffer both
channels generously if reliable delivery matters. BulkCreate's own returned error is always
the authoritative signal that an operation failed — ErrorCh only adds detail and must never
be treated as the sole source of truth.

Callers must never close MetricsCh or ErrorCh, not even after BulkCreate returns. On a
cancelled context the underlying bulk indexer does not wait for its workers to fully join,
so a callback can fire briefly after BulkCreate returns; sending on an already-closed
channel panics the process. Simply stop reading from the channel instead — there is
nothing to clean up.
*/
package ebi
