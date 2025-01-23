package ebi

import (
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/ebi/internal/shared"
)

//////
// Const, vars, types.
//////

const (
	ErrDocumentsRequired                = "ERR_DOCUMENTS_REQUIRED"                     // Required.
	ErrFailedToAddDocumentToBulkIndexer = "ERR_FAILED_TO_ADD_DOCUMENT_TO_BULK_INDEXER" // FailedTo.
	ErrFailedToBulkIndexDocuments       = "ERR_FAILED_TO_BULK_INDEX_DOCUMENTS"         // FailedTo.
	ErrFailedToConvertToJSON            = "ERR_FAILED_TO_CONVERT_TO_JSON"              // FailedTo.
	ErrFailedToCreateBulkIndexer        = "ERR_FAILED_TO_CREATE_BULK_INDEXER"          // FailedTo.
	ErrFailedToCreateClient             = "ERR_FAILED_TO_CREATE_CLIENT"                // FailedTo.
	ErrFailedToIndexDocument            = "ERR_FAILED_TO_INDEX_DOCUMENT"               // FailedTo.
	ErrFailedToPing                     = "ERR_FAILED_TO_PING"                         // FailedTo.
	ErrFailedToRefreshIndex             = "ERR_FAILED_TO_REFRESH_INDEX"                // FailedTo.
	ErrFailedToUpdateMetrics            = "ERR_FAILED_TO_UPDATE_METRICS"               // FailedTo.
	ErrIndexerError                     = "ERR_INDEXER_ERROR"                          // New.
	ErrIndexNameRequired                = "ERR_INDEX_NAME_REQUIRED"                    // Required.
	ErrInvalidBulkOptions               = "ERR_INVALID_BULK_OPTIONS"                   // Invalid.
)

// ErrorCatalog is the error catalog for the CLI.
var ErrorCatalog = customerror.
	MustNewCatalog(shared.Name).
	MustSet(ErrDocumentsRequired, "documents").
	MustSet(ErrFailedToAddDocumentToBulkIndexer, "add document to bulk indexer").
	MustSet(ErrFailedToBulkIndexDocuments, "bulk index documents").
	MustSet(ErrFailedToConvertToJSON, "convert to JSON").
	MustSet(ErrFailedToCreateBulkIndexer, "create bulk indexer").
	MustSet(ErrFailedToCreateClient, "create client").
	MustSet(ErrFailedToIndexDocument, "index document").
	MustSet(ErrFailedToPing, "ping").
	MustSet(ErrFailedToRefreshIndex, "refresh index").
	MustSet(ErrFailedToUpdateMetrics, "update metrics").
	MustSet(ErrIndexerError, "indexer error").
	MustSet(ErrIndexNameRequired, "index name").
	MustSet(ErrInvalidBulkOptions, "bulk options")

//////
// Exported functionalities.
//////

// MustGet returns a custom error from the error catalog.
func MustGet(errorCode string, opts ...customerror.Option) *customerror.CustomError {
	return ErrorCatalog.MustGet(errorCode, opts...)
}
