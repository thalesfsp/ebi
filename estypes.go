package ebi

//////
// Indices stats.
//////

// IndexStatsResponse represents the top-level response from Elasticsearch index stats API.
type IndexStatsResponse struct {
	Indices map[string]IndexStats `json:"indices"`
}

// ShardStats contains information about shards.
type ShardStats struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

// AllStats contains aggregated stats across all indices.
type AllStats struct {
	Primaries *DetailedStats `json:"primaries"`
	Total     *DetailedStats `json:"total"`
}

// IndexStats contains stats for a specific index.
type IndexStats struct {
	Health string         `json:"health"`
	Status string         `json:"status"`
	UUID   string         `json:"uuid"`
	Total  *DetailedStats `json:"total"`
}

// DetailedStats contains the detailed statistics.
type DetailedStats struct {
	Indexing *IndexingStats `json:"indexing"`
	Segments *SegmentStats  `json:"segments"`
	Translog *TranslogStats `json:"translog"`
}

// SegmentStats contains segment-related statistics.
type SegmentStats struct {
	Count                     int                    `json:"count"`
	DocValuesMemoryInBytes    int64                  `json:"doc_values_memory_in_bytes"`
	FileSizes                 map[string]interface{} `json:"file_sizes"`
	FixedBitSetMemoryInBytes  int64                  `json:"fixed_bit_set_memory_in_bytes"`
	IndexWriterMemoryInBytes  int64                  `json:"index_writer_memory_in_bytes"`
	MaxUnsafeAutoIDTimestamp  int64                  `json:"max_unsafe_auto_id_timestamp"`
	MemoryInBytes             int64                  `json:"memory_in_bytes"`
	NormsMemoryInBytes        int64                  `json:"norms_memory_in_bytes"`
	PointsMemoryInBytes       int64                  `json:"points_memory_in_bytes"`
	StoredFieldsMemoryInBytes int64                  `json:"stored_fields_memory_in_bytes"`
	TermVectorsMemoryInBytes  int64                  `json:"term_vectors_memory_in_bytes"`
	TermsMemoryInBytes        int64                  `json:"terms_memory_in_bytes"`
	VersionMapMemoryInBytes   int64                  `json:"version_map_memory_in_bytes"`
}

// TranslogStats contains translog-related statistics.
type TranslogStats struct {
	EarliestLastModifiedAge int64 `json:"earliest_last_modified_age"`
	Operations              int64 `json:"operations"`
	SizeInBytes             int64 `json:"size_in_bytes"`
	UncommittedOperations   int64 `json:"uncommitted_operations"`
	UncommittedSizeInBytes  int64 `json:"uncommitted_size_in_bytes"`
}

//////
// Nodes stats.
//////

// NodesStats represents the top-level response from Elasticsearch nodes stats API.
type NodesStats struct {
	ClusterName string                  `json:"cluster_name"`
	NodeStats   *NodeStatsShardInfo     `json:"_nodes"`
	Nodes       map[string]*NodeDetails `json:"nodes"`
}

// NodeStatsShardInfo contains shard-related statistics.
type NodeStatsShardInfo struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type MemoryStats struct {
	TotalSizeInBytes int64 `json:"total_in_bytes"`
}

type OSStats struct {
	Memory *MemoryStats `json:"memory"`
}

// NodeDetails contains detailed information about a specific node.
type NodeDetails struct {
	Name             string                 `json:"name"`
	TransportAddress string                 `json:"transport_address"`
	Host             string                 `json:"host"`
	IP               string                 `json:"ip"`
	Roles            []string               `json:"roles"`
	Attributes       map[string]string      `json:"attributes"`
	Timestamp        int64                  `json:"timestamp"`
	JVM              *JVMStats              `json:"jvm"`
	Breakers         *BreakerStats          `json:"breakers"`
	IndexingPressure *IndexingPressureStats `json:"indexing_pressure"`
	Indices          *IndicesStats          `json:"indices"`
	OS               *OSStats               `json:"os"`
}

// IndicesStats contains statistics for indices.
type IndicesStats struct {
	Bulk         *BulkStats         `json:"bulk"`
	Completion   *CompletionStats   `json:"completion"`
	Docs         *DocsStats         `json:"docs"`
	Fielddata    *FielddataStats    `json:"fielddata"`
	Flush        *FlushStats        `json:"flush"`
	Get          *GetStats          `json:"get"`
	Indexing     *IndexingStats     `json:"indexing"`
	Merges       *MergesStats       `json:"merges"`
	QueryCache   *QueryCacheStats   `json:"query_cache"`
	Recovery     *RecoveryStats     `json:"recovery"`
	Refresh      *RefreshStats      `json:"refresh"`
	RequestCache *RequestCacheStats `json:"request_cache"`
	Search       *SearchStats       `json:"search"`
	Segments     *SegmentsStats     `json:"segments"`
	Store        *StoreStats        `json:"store"`
	Translog     *TranslogStats     `json:"translog"`
	Warmer       *WarmerStats       `json:"warmer"`
	Mappings     *MappingsStats     `json:"mappings"`
}

// BulkStats contains bulk-related statistics.
type BulkStats struct {
	TotalOperations   int64   `json:"total_operations"`
	TotalTimeInMillis int64   `json:"total_time_in_millis"`
	TotalSizeInBytes  int64   `json:"total_size_in_bytes"`
	AvgTimeInMillis   float64 `json:"avg_time_in_millis"`
	AvgSizeInBytes    float64 `json:"avg_size_in_bytes"`
}

// CompletionStats contains completion-related statistics.
type CompletionStats struct {
	SizeInBytes int64 `json:"size_in_bytes"`
}

// DocsStats contains document-related statistics.
type DocsStats struct {
	Count            int64 `json:"count"`
	Deleted          int64 `json:"deleted"`
	TotalSizeInBytes int64 `json:"total_size_in_bytes"`
}

// FielddataStats contains fielddata-related statistics.
type FielddataStats struct {
	Evictions         int64                `json:"evictions"`
	MemorySizeInBytes int64                `json:"memory_size_in_bytes"`
	GlobalOrdinals    *GlobalOrdinalsStats `json:"global_ordinals"`
}

// GlobalOrdinalsStats contains global ordinals statistics.
type GlobalOrdinalsStats struct {
	BuildTimeInMillis int64 `json:"build_time_in_millis"`
}

// FlushStats contains flush-related statistics.
type FlushStats struct {
	Total                                   int64 `json:"total"`
	Periodic                                int64 `json:"periodic"`
	TotalTimeInMillis                       int64 `json:"total_time_in_millis"`
	TotalTimeExcludingWaitingOnLockInMillis int64 `json:"total_time_excluding_waiting_on_lock_in_millis"`
}

// GetStats contains get-related statistics.
type GetStats struct {
	Current             int64 `json:"current"`
	ExistsTotal         int64 `json:"exists_total"`
	ExistsTimeInMillis  int64 `json:"exists_time_in_millis"`
	MissingTotal        int64 `json:"missing_total"`
	MissingTimeInMillis int64 `json:"missing_time_in_millis"`
	Total               int64 `json:"total"`
	TimeInMillis        int64 `json:"time_in_millis"`
}

// IndexingStats contains indexing-related statistics.
type IndexingStats struct {
	IndexTotal           int64   `json:"index_total"`
	IndexTimeInMillis    int64   `json:"index_time_in_millis"`
	IndexCurrent         int64   `json:"index_current"`
	IndexFailed          int64   `json:"index_failed"`
	DeleteTotal          int64   `json:"delete_total"`
	DeleteTimeInMillis   int64   `json:"delete_time_in_millis"`
	DeleteCurrent        int64   `json:"delete_current"`
	NoopUpdateTotal      int64   `json:"noop_update_total"`
	IsThrottled          bool    `json:"is_throttled"`
	ThrottleTimeInMillis int64   `json:"throttle_time_in_millis"`
	WriteLoad            float64 `json:"write_load"`
}

// MergesStats contains merge-related statistics.
type MergesStats struct {
	Current                    int64 `json:"current"`
	CurrentDocs                int64 `json:"current_docs"`
	CurrentSizeInBytes         int64 `json:"current_size_in_bytes"`
	Total                      int64 `json:"total"`
	TotalTimeInMillis          int64 `json:"total_time_in_millis"`
	TotalDocs                  int64 `json:"total_docs"`
	TotalSizeInBytes           int64 `json:"total_size_in_bytes"`
	TotalStoppedTimeInMillis   int64 `json:"total_stopped_time_in_millis"`
	TotalThrottledTimeInMillis int64 `json:"total_throttled_time_in_millis"`
	TotalAutoThrottleInBytes   int64 `json:"total_auto_throttle_in_bytes"`
}

// QueryCacheStats contains query cache statistics.
type QueryCacheStats struct {
	MemorySizeInBytes int64 `json:"memory_size_in_bytes"`
	TotalCount        int64 `json:"total_count"`
	HitCount          int64 `json:"hit_count"`
	MissCount         int64 `json:"miss_count"`
	CacheSize         int64 `json:"cache_size"`
	CacheCount        int64 `json:"cache_count"`
	Evictions         int64 `json:"evictions"`
}

// RecoveryStats contains recovery-related statistics.
type RecoveryStats struct {
	CurrentAsSource      int64 `json:"current_as_source"`
	CurrentAsTarget      int64 `json:"current_as_target"`
	ThrottleTimeInMillis int64 `json:"throttle_time_in_millis"`
}

// RefreshStats contains refresh-related statistics.
type RefreshStats struct {
	Total                     int64 `json:"total"`
	TotalTimeInMillis         int64 `json:"total_time_in_millis"`
	ExternalTotal             int64 `json:"external_total"`
	ExternalTotalTimeInMillis int64 `json:"external_total_time_in_millis"`
	Listeners                 int64 `json:"listeners"`
}

// RequestCacheStats contains request cache statistics.
type RequestCacheStats struct {
	MemorySizeInBytes int64 `json:"memory_size_in_bytes"`
	HitCount          int64 `json:"hit_count"`
	MissCount         int64 `json:"miss_count"`
	Evictions         int64 `json:"evictions"`
}

// SearchStats contains search-related statistics.
type SearchStats struct {
	OpenContexts        int64 `json:"open_contexts"`
	QueryTotal          int64 `json:"query_total"`
	QueryTimeInMillis   int64 `json:"query_time_in_millis"`
	QueryCurrent        int64 `json:"query_current"`
	FetchTotal          int64 `json:"fetch_total"`
	FetchTimeInMillis   int64 `json:"fetch_time_in_millis"`
	FetchCurrent        int64 `json:"fetch_current"`
	ScrollTotal         int64 `json:"scroll_total"`
	ScrollTimeInMillis  int64 `json:"scroll_time_in_millis"`
	ScrollCurrent       int64 `json:"scroll_current"`
	SuggestTotal        int64 `json:"suggest_total"`
	SuggestTimeInMillis int64 `json:"suggest_time_in_millis"`
	SuggestCurrent      int64 `json:"suggest_current"`
}

// SegmentsStats contains segment-related statistics.
type SegmentsStats struct {
	Count                     int64                  `json:"count"`
	DocValuesMemoryInBytes    int64                  `json:"doc_values_memory_in_bytes"`
	FileSizes                 map[string]interface{} `json:"file_sizes"`
	FixedBitSetMemoryInBytes  int64                  `json:"fixed_bit_set_memory_in_bytes"`
	IndexWriterMemoryInBytes  int64                  `json:"index_writer_memory_in_bytes"`
	MaxUnsafeAutoIDTimestamp  int64                  `json:"max_unsafe_auto_id_timestamp"`
	Memory                    int64                  `json:"memory_in_bytes"`
	NormsMemoryInBytes        int64                  `json:"norms_memory_in_bytes"`
	PointsMemoryInBytes       int64                  `json:"points_memory_in_bytes"`
	StoredFieldsMemoryInBytes int64                  `json:"stored_fields_memory_in_bytes"`
	TermsMemoryInBytes        int64                  `json:"terms_memory_in_bytes"`
	TermVectorsMemoryInBytes  int64                  `json:"term_vectors_memory_in_bytes"`
	VersionMapMemoryInBytes   int64                  `json:"version_map_memory_in_bytes"`
}

// StoreStats contains store-related statistics.
type StoreStats struct {
	SizeInBytes             int64 `json:"size_in_bytes"`
	ReservedInBytes         int64 `json:"reserved_in_bytes"`
	TotalDataSetSizeInBytes int64 `json:"total_data_set_size_in_bytes"`
}

// WarmerStats contains warmer-related statistics.
type WarmerStats struct {
	Current           int64 `json:"current"`
	Total             int64 `json:"total"`
	TotalTimeInMillis int64 `json:"total_time_in_millis"`
}

// MappingsStats contains mapping-related statistics.
type MappingsStats struct {
	TotalCount                    int64 `json:"total_count"`
	TotalEstimatedOverheadInBytes int64 `json:"total_estimated_overhead_in_bytes"`
	TotalSegmentFields            int64 `json:"total_segment_fields"`
	AverageFieldsPerSegment       int64 `json:"average_fields_per_segment"`
	TotalSegments                 int64 `json:"total_segments"`
}

// IndexingPressureStats contém as estatísticas de pressão de indexação.
type IndexingPressureStats struct {
	Memory *IndexingPressureMemory `json:"memory"`
}

// IndexingPressureMemory contém métricas de memória relacionadas à indexação.
type IndexingPressureMemory struct {
	Current      *IndexingPressureMemoryUsage `json:"current"`
	Total        *IndexingPressureMemoryUsage `json:"total"`
	LimitInBytes int64                        `json:"limit_in_bytes"`
}

// IndexingPressureMemoryUsage contém detalhes do uso de memória para indexação.
type IndexingPressureMemoryUsage struct {
	AllInBytes                            int64 `json:"all_in_bytes"`
	CombinedCoordinatingAndPrimaryInBytes int64 `json:"combined_coordinating_and_primary_in_bytes"`
	CoordinatingInBytes                   int64 `json:"coordinating_in_bytes"`
	PrimaryInBytes                        int64 `json:"primary_in_bytes"`
	ReplicaInBytes                        int64 `json:"replica_in_bytes"`
	CoordinatingRejections                int64 `json:"coordinating_rejections,omitempty"`
	PrimaryRejections                     int64 `json:"primary_rejections,omitempty"`
	ReplicaRejections                     int64 `json:"replica_rejections,omitempty"`
	PrimaryDocumentRejections             int64 `json:"primary_document_rejections,omitempty"`
}

// JVMStats contains JVM-related statistics.
type JVMStats struct {
	Timestamp      int64                  `json:"timestamp"`
	UptimeInMillis int64                  `json:"uptime_in_millis"`
	Mem            *JVMMemoryStats        `json:"mem"`
	BufferPools    map[string]*BufferPool `json:"buffer_pools"`
	GC             *GCStats               `json:"gc"`
	Threads        *ThreadStats           `json:"threads"`
	Classes        *ClassLoadingStats     `json:"classes"`
}

// JVMMemoryStats contains JVM memory statistics.
type JVMMemoryStats struct {
	HeapUsedInBytes         int64                  `json:"heap_used_in_bytes"`
	HeapUsedPercent         int                    `json:"heap_used_percent"`
	HeapCommittedInBytes    int64                  `json:"heap_committed_in_bytes"`
	HeapMaxInBytes          int64                  `json:"heap_max_in_bytes"`
	NonHeapUsedInBytes      int64                  `json:"non_heap_used_in_bytes"`
	NonHeapCommittedInBytes int64                  `json:"non_heap_committed_in_bytes"`
	Pools                   map[string]*MemoryPool `json:"pools"`
}

// MemoryPool contains memory pool statistics.
type MemoryPool struct {
	UsedInBytes     int64 `json:"used_in_bytes"`
	MaxInBytes      int64 `json:"max_in_bytes"`
	PeakUsedInBytes int64 `json:"peak_used_in_bytes"`
	PeakMaxInBytes  int64 `json:"peak_max_in_bytes"`
}

// BufferPool contains buffer pool statistics.
type BufferPool struct {
	Count                int64 `json:"count"`
	UsedInBytes          int64 `json:"used_in_bytes"`
	TotalCapacityInBytes int64 `json:"total_capacity_in_bytes"`
}

// GCStats contains garbage collection statistics.
type GCStats struct {
	Collectors map[string]*GCCollector `json:"collectors"`
}

// GCCollector contains statistics for a specific garbage collector.
type GCCollector struct {
	CollectionCount        int64 `json:"collection_count"`
	CollectionTimeInMillis int64 `json:"collection_time_in_millis"`
}

// ThreadStats contains thread-related statistics.
type ThreadStats struct {
	Count     int `json:"count"`
	PeakCount int `json:"peak_count"`
}

// ClassLoadingStats contains class loading statistics.
type ClassLoadingStats struct {
	CurrentLoadedCount int `json:"current_loaded_count"`
	TotalLoadedCount   int `json:"total_loaded_count"`
	TotalUnloadedCount int `json:"total_unloaded_count"`
}

// BreakerStats contains circuit breaker statistics.
type BreakerStats struct {
	Parent           *BreakerDetails `json:"parent"`
	FieldData        *BreakerDetails `json:"fielddata"`
	InFlightRequests *BreakerDetails `json:"inflight_requests"`
	ModelInference   *BreakerDetails `json:"model_inference"`
	EQLSequence      *BreakerDetails `json:"eql_sequence"`
	Request          *BreakerDetails `json:"request"`
}

// BreakerDetails contains detailed statistics for a circuit breaker.
type BreakerDetails struct {
	EstimatedSizeInBytes int64   `json:"estimated_size_in_bytes"`
	LimitSizeInBytes     int64   `json:"limit_size_in_bytes"`
	Overhead             float64 `json:"overhead"`
	Tripped              int64   `json:"tripped"`
}
