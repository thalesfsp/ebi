package ebi

import (
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/ebi/internal/shared"
)

//////
// Const, vars, types.
//////

const (
	ErrContextCancelled                 = "ERR_CONTEXT_CANCELLED"                      // FailedTo.
	ErrDocumentsRequired                = "ERR_DOCUMENTS_REQUIRED"                     // Required.
	ErrFailedToAddDocumentToBulkIndexer = "ERR_FAILED_TO_ADD_DOCUMENT_TO_BULK_INDEXER" // FailedTo.
	ErrFailedToBulkIndexDocuments       = "ERR_FAILED_TO_BULK_INDEX_DOCUMENTS"         // FailedTo.
	ErrFailedToConvertToJSON            = "ERR_FAILED_TO_CONVERT_TO_JSON"              // FailedTo.
	ErrFailedToCreateBulkIndexer        = "ERR_FAILED_TO_CREATE_BULK_INDEXER"          // FailedTo.
	ErrFailedToCreateClient             = "ERR_FAILED_TO_CREATE_CLIENT"                // FailedTo.
	ErrFailedToCreateMetrics            = "ERR_FAILED_TO_CREATE_METRICS"               // FailedTo.
	ErrFailedToDiscoverNodes            = "ERR_FAILED_TO_DISCOVER_NODES"               // FailedTo.
	ErrFailedToGetNodeStats             = "ERR_FAILED_TO_GET_NODE_STATS"               // FailedTo.
	ErrFailedToIndexDocument            = "ERR_FAILED_TO_INDEX_DOCUMENT"               // FailedTo.
	ErrFailedToParseNodeStats           = "ERR_FAILED_TO_PARSE_NODE_STATS"             // FailedTo.
	ErrFailedToPing                     = "ERR_FAILED_TO_PING"                         // FailedTo.
	ErrFailedToRefreshIndex             = "ERR_FAILED_TO_REFRESH_INDEX"                // FailedTo.
	ErrFailedToRetrieveNumWorkers       = "ERR_FAILED_TO_RETRIEVE_NUM_WORKERS"         // FailedTo.
	ErrFailedToUpdateMetrics            = "ERR_FAILED_TO_UPDATE_METRICS"               // FailedTo.
	ErrHyperparameterOptimization       = "ERR_HYPERPARAMETER_OPTIMIZATION"            // FailedTo.
	ErrIndexerError                     = "ERR_INDEXER_ERROR"                          // New.
	ErrIndexNameRequired                = "ERR_INDEX_NAME_REQUIRED"                    // Required.
	ErrInvalidBulkOptions               = "ERR_INVALID_BULK_OPTIONS"                   // Invalid.
	ErrInvalidMetrics                   = "ERR_INVALID_METRICS"                        // Invalid.
	ErrInvalidOperation                 = "ERR_INVALID_OPERATION"                      // Invalid.
)

// ErrorCatalog is the error catalog for the EBI package.
var ErrorCatalog = customerror.
	MustNewCatalog(shared.Name).
	MustSet(ErrContextCancelled, "context cancelled").
	MustSet(ErrDocumentsRequired, "documents").
	MustSet(ErrFailedToAddDocumentToBulkIndexer, "add document to bulk indexer").
	MustSet(ErrFailedToBulkIndexDocuments, "bulk index documents").
	MustSet(ErrFailedToConvertToJSON, "convert to JSON").
	MustSet(ErrFailedToCreateBulkIndexer, "create bulk indexer").
	MustSet(ErrFailedToCreateClient, "create client").
	MustSet(ErrFailedToCreateMetrics, "create metrics").
	MustSet(ErrFailedToDiscoverNodes, "discover worker nodes").
	MustSet(ErrFailedToGetNodeStats, "get node stats").
	MustSet(ErrFailedToIndexDocument, "index document").
	MustSet(ErrFailedToParseNodeStats, "parse node stats").
	MustSet(ErrFailedToPing, "ping").
	MustSet(ErrFailedToRefreshIndex, "refresh index").
	MustSet(ErrFailedToRetrieveNumWorkers, "retrieve number of worker nodes").
	MustSet(ErrFailedToUpdateMetrics, "update metrics").
	MustSet(ErrHyperparameterOptimization, "hyperparameter optimization").
	MustSet(ErrIndexerError, "indexer error").
	MustSet(ErrIndexNameRequired, "index name").
	MustSet(ErrInvalidBulkOptions, "bulk options").
	MustSet(ErrInvalidMetrics, "metrics").
	MustSet(ErrInvalidOperation, "operation")

//////
// Exported functionalities.
//////

// MustGet returns a custom error from the error catalog.
func MustGet(errorCode string, opts ...customerror.Option) *customerror.CustomError {
	return ErrorCatalog.MustGet(errorCode, opts...)
}
