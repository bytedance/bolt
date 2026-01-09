/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "bolt/common/config/Config.h"
#include "bolt/core/Config.h"
namespace bytedance::bolt::core {
/// A simple wrapper around bolt::Config. Defines constants for query
/// config properties and accessor methods.
/// Create per query context. Does not have a singleton instance.
/// Does not allow altering properties on the fly. Only at creation time.
class QueryConfig {
 public:
  explicit QueryConfig(
      const std::unordered_map<std::string, std::string>& values);

  explicit QueryConfig(std::unordered_map<std::string, std::string>&& values);

  static constexpr const char* kCodegenEnabled = "codegen.enabled";

  /// Maximum memory that a query can use on a single host.
  static constexpr const char* kQueryMaxMemoryPerNode =
      "query_max_memory_per_node";

  static constexpr const char* kCodegenConfigurationFilePath =
      "codegen.configuration_file_path";

  static constexpr const char* kCodegenLazyLoading = "codegen.lazy_loading";

  /// User provided session timezone. Stores a string with the actual timezone
  /// name, e.g: "America/Los_Angeles".
  static constexpr const char* kSessionTimezone = "session_timezone";

  /// If true, timezone-less timestamp conversions (e.g. string to timestamp,
  /// when the string does not specify a timezone) will be adjusted to the user
  /// provided session timezone (if any).
  ///
  /// For instance:
  ///
  ///  if this option is true and user supplied "America/Los_Angeles",
  ///  "1970-01-01" will be converted to -28800 instead of 0.
  ///
  /// False by default.
  static constexpr const char* kAdjustTimestampToTimezone =
      "adjust_timestamp_to_session_timezone";

  /// Whether to use the simplified expression evaluation path. False by
  /// default.
  static constexpr const char* kExprEvalSimplified =
      "expression.eval_simplified";

  /// Whether to track CPU usage for individual expressions (supported by call
  /// and cast expressions). False by default. Can be expensive when processing
  /// small batches, e.g. < 10K rows.
  static constexpr const char* kExprTrackCpuUsage =
      "expression.track_cpu_usage";

  /// Whether to track CPU usage for stages of individual operators. True by
  /// default. Can be expensive when processing small batches, e.g. < 10K rows.
  static constexpr const char* kOperatorTrackCpuUsage =
      "track_operator_cpu_usage";

  /// Flags used to configure the CAST operator:

  static constexpr const char* kLegacyCast = "legacy_cast";

  /// Flags used to configure the CAST complex type to string;
  static constexpr const char* kSparkLegacyCastComplexTypesToStringEnabled =
      "spark.legacy_cast_complex_types_to_string_enabled";

  /// This flag makes the Row conversion to by applied in a way that the casting
  /// row field are matched by name instead of position.
  static constexpr const char* kCastMatchStructByName =
      "cast_match_struct_by_name";

  /// If set, cast from float/double/decimal/string to integer truncates the
  /// decimal part, otherwise rounds.
  static constexpr const char* kCastToIntByTruncate = "cast_to_int_by_truncate";
  static constexpr const char* kSpecTimezone = "spec_timezone";

  /// If set, cast from string to date allows only ISO 8601 formatted strings:
  /// [+-](YYYY-MM-DD). Otherwise, allows all patterns supported by Spark:
  /// `[+-]yyyy*`
  /// `[+-]yyyy*-[m]m`
  /// `[+-]yyyy*-[m]m-[d]d`
  /// `[+-]yyyy*-[m]m-[d]d *`
  /// `[+-]yyyy*-[m]m-[d]dT*`
  /// The asterisk `*` in `yyyy*` stands for any numbers.
  /// For the last two patterns, the trailing `*` can represent none or any
  /// sequence of characters, e.g:
  ///   "1970-01-01 123"
  ///   "1970-01-01 (BC)"
  static constexpr const char* kCastStringToDateIsIso8601 =
      "cast_string_to_date_is_iso_8601";

  /// Used for backpressure to block local exchange producers when the local
  /// exchange buffer reaches or exceeds this size.
  static constexpr const char* kMaxLocalExchangeBufferSize =
      "max_local_exchange_buffer_size";

  /// Maximum size in bytes to accumulate in ExchangeQueue. Enforced
  /// approximately, not strictly.
  static constexpr const char* kMaxExchangeBufferSize =
      "exchange.max_buffer_size";

  /// Maximum size in bytes to accumulate among all sources of the merge
  /// exchange. Enforced approximately, not strictly.
  static constexpr const char* kMaxMergeExchangeBufferSize =
      "merge_exchange.max_buffer_size";

  static constexpr const char* kMaxMemoryPerQuery = "max_query_memory";

  static constexpr const char* kMaxPartialAggregationMemory =
      "max_partial_aggregation_memory";

  static constexpr const char* kMaxExtendedPartialAggregationMemory =
      "max_extended_partial_aggregation_memory";

  static constexpr const char* kAbandonPartialAggregationMinRows =
      "abandon_partial_aggregation_min_rows";

  static constexpr const char* kAbandonPartialAggregationMinPct =
      "abandon_partial_aggregation_min_pct";

  static constexpr const char* kAbandonPartialAggregationMinFinalPct =
      "abandon_partial_aggregation_min_final_pct";

  static constexpr const char* kPartialAggregationSpillMaxPct =
      "partial_aggregation_spill_max_pct";

  static constexpr const char* kPreferPartialAggregationSpill =
      "prefer_partial_aggregation_spill";

  static constexpr const char* kHashAggregationCompositeOutputEnabled =
      "hashaggregation_composite_output_enabled";

  static constexpr const char* kHashAggregationUniqueRowOpt =
      "hashaggregation_unique_row_opt";

  static constexpr const char* kHashAggregationCompositeOutputAccumulatorRatio =
      "hashaggregation_composite_output_accumulator_ratio";

  static constexpr const char* kAdaptiveSkippedDataSizeThreshold =
      "adaptive_skipped_datasize_threshold";

  static constexpr const char* kAbandonPartialTopNRowNumberMinRows =
      "abandon_partial_topn_row_number_min_rows";

  static constexpr const char* kAbandonPartialTopNRowNumberMinPct =
      "abandon_partial_topn_row_number_min_pct";

  static constexpr const char* kMaxPartitionedOutputBufferSize =
      "max_page_partitioning_buffer_size";

  /// Deprecated. Use kMaxOutputBufferSize instead.
  static constexpr const char* kMaxArbitraryBufferSize =
      "max_arbitrary_buffer_size";

  static constexpr const char* kMaxOutputBufferSize = "max_output_buffer_size";

  /// Preferred size of batches in bytes to be returned by operators from
  /// Operator::getOutput. It is used when an estimate of average row size is
  /// known. Otherwise kPreferredOutputBatchRows is used.
  static constexpr const char* kPreferredOutputBatchBytes =
      "preferred_output_batch_bytes";

  /// Preferred number of rows to be returned by operators from
  /// Operator::getOutput. It is used when an estimate of average row size is
  /// not known. When the estimate of average row size is known,
  /// kPreferredOutputBatchBytes is used.
  static constexpr const char* kPreferredOutputBatchRows =
      "preferred_output_batch_rows";

  /// Max number of rows that could be return by operators from
  /// Operator::getOutput. It is used when an estimate of average row size is
  /// known and kPreferredOutputBatchBytes is used to compute the number of
  /// output rows.
  static constexpr const char* kMaxOutputBatchRows = "max_output_batch_rows";

  static constexpr const char* kMinOutputBatchRows = "min_output_batch_rows";

  /// TableScan operator will exit getOutput() method after this many
  /// milliseconds even if it has no data to return yet. Zero means 'no time
  /// limit'.
  static constexpr const char* kTableScanGetOutputTimeLimitMs =
      "table_scan_getoutput_time_limit_ms";

  /// If false, the 'group by' code is forced to use generic hash mode
  /// hashtable.
  static constexpr const char* kHashAdaptivityEnabled =
      "hash_adaptivity_enabled";

  /// If true, the conjunction expression can reorder inputs based on the time
  /// taken to calculate them.
  static constexpr const char* kAdaptiveFilterReorderingEnabled =
      "adaptive_filter_reordering_enabled";

  /// Global enable spilling flag.
  static constexpr const char* kSpillEnabled = "spill_enabled";

  /// Aggregation spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kAggregationSpillEnabled =
      "aggregation_spill_enabled";

  static constexpr const char* kHllSketchRounded = "hll_sketch_rounded";

  static constexpr const char* kSpilledAggregationBypassHTRatio =
      "spilled_aggregation_bypass_hashtable_ratio";

  /// Join spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kJoinSpillEnabled = "join_spill_enabled";

  /// OrderBy spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kOrderBySpillEnabled = "order_by_spill_enabled";

  /// Support orderBy spilling in output stage
  static constexpr const char* kOrderBySpillInOutputStageEnabled =
      "order_by_spill_output_stage_enabled";

  /// Window spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kWindowSpillEnabled = "window_spill_enabled";

  /// If true, the memory arbitrator will reclaim memory from table writer by
  /// flushing its buffered data to disk.
  static constexpr const char* kWriterSpillEnabled = "writer_spill_enabled";

  /// RowNumber spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kRowNumberSpillEnabled =
      "row_number_spill_enabled";

  /// TopNRowNumber spilling flag, only applies if "spill_enabled" flag is set.
  static constexpr const char* kTopNRowNumberSpillEnabled =
      "topn_row_number_spill_enabled";

  /// Export to StringView flag. true means export to StringView type.
  static constexpr const char* kExportToStringViewEnabled =
      "export_to_string_view_enabled";

  /// flattenDictionary flag for ES use case. If true, exporting dictionary will
  /// be flattened.
  static constexpr const char* kExportFlattenDictionaryEnabled =
      "export_flatten_dictionary_enabled";

  /// flattenConstant flag for ES use case. If true, exporting constant type
  /// will be flattened.
  static constexpr const char* kExportFlattenConstantEnabled =
      "export_flatten_constant_enabled";

  /// The max memory that a final aggregation can use before spilling. If it 0,
  /// then there is no limit.
  static constexpr const char* kAggregationSpillMemoryThreshold =
      "aggregation_spill_memory_threshold";

  /// The max memory that a hash join can use before spilling. If it 0, then
  /// there is no limit.
  static constexpr const char* kJoinSpillMemoryThreshold =
      "join_spill_memory_threshold";

  /// The max memory that an order by can use before spilling. If it 0, then
  /// there is no limit.
  static constexpr const char* kOrderBySpillMemoryThreshold =
      "order_by_spill_memory_threshold";

  /// The threshold for enabling LZ4 spill compression.
  static constexpr const char* kSpillLowCompressByteThreshold =
      "spill_low_compress_byte_threshold";

  /// The threshold for enabling zstd spill compression.
  static constexpr const char* kSpillHighCompressByteThreshold =
      "spill_high_compress_byte_threshold";

  /// The max row numbers to fill and spill for each spill run. This is used to
  /// cap the memory used for spilling. If it is zero, then there is no limit
  /// and spilling might run out of memory.
  /// Based on offline test results, the default value is set to 12 million rows
  /// which uses ~128MB memory when to fill a spill run.
  static constexpr const char* kMaxSpillRunRows = "max_spill_run_rows";

  /// The max spill bytes limit set for each query. This is used to cap the
  /// storage used for spilling. If it is zero, then there is no limit and
  /// spilling might exhaust the storage or takes too long to run. The default
  /// value is set to 100 GB.
  static constexpr const char* kMaxSpillBytes = "max_spill_bytes";

  static constexpr const char* kTestingSpillPct = "testing.spill_pct";

  /// The max allowed spilling level with zero being the initial spilling level.
  /// This only applies for hash build spilling which might trigger recursive
  /// spilling when the build table is too big. If it is set to -1, then there
  /// is no limit and then some extreme large query might run out of spilling
  /// partition bits (see kSpillPartitionBits) at the end. The max spill level
  /// is used in production to prevent some bad user queries from using too much
  /// io and cpu resources.
  static constexpr const char* kMaxSpillLevel = "max_spill_level";

  /// The max allowed spill file size. If it is zero, then there is no limit.
  static constexpr const char* kMaxSpillFileSize = "max_spill_file_size";

  /// The min spill run size limit used to select partitions for spilling. The
  /// spiller tries to spill a previously spilled partitions if its data size
  /// exceeds this limit, otherwise it spills the partition with most data.
  /// If the limit is zero, then the spiller always spill a previously spilled
  /// partition if it has any data. This is to avoid spill from a partition with
  /// a small amount of data which might result in generating too many small
  /// spilled files.
  static constexpr const char* kMinSpillRunSize = "min_spill_run_size";

  static constexpr const char* kSpillCompressionKind =
      "spill_compression_codec";

  /// Specifies spill write buffer size in bytes. The spiller tries to buffer
  /// serialized spill data up to the specified size before write to storage
  /// underneath for io efficiency. If it is set to zero, then spill write
  /// buffering is disabled.
  static constexpr const char* kSpillWriteBufferSize =
      "spill_write_buffer_size";

  // for io_uring
  static constexpr const char* kSpillUringEnabled = "spill_uring_enabled";

  /// Config used to create spill files. This config is provided to underlying
  /// file system and the config is free form. The form should be defined by the
  /// underlying file system.
  static constexpr const char* kSpillFileCreateConfig =
      "spill_file_create_config";

  /// Vector serde used for row-vector spill when single partition is used.
  static constexpr const char* kSinglePartitionSpillSerdeKind =
      "spill_single_partition_serde_kind";

  static constexpr const char* kSpillStartPartitionBit =
      "spiller_start_partition_bit";

  /// Default number of spill partition bits. It is the number of bits used to
  /// calculate the spill partition number for hash join and RowNumber. The
  /// number of spill partitions will be power of two.
  ///
  /// NOTE: as for now, we only support up to 8-way spill partitioning.
  static constexpr const char* kSpillNumPartitionBits =
      "spiller_num_partition_bits";

  static constexpr const char* kMinSpillableReservationPct =
      "min_spillable_reservation_pct";

  static constexpr const char* kSpillableReservationGrowthPct =
      "spillable_reservation_growth_pct";

  static constexpr const char* kRowBasedSpillMode = "row_based_spill_mode";

  static constexpr const char* kDefaultRowBasedSpillMode = "compression";

  /// Minimum memory footprint size required to reclaim memory from a file
  /// writer by flushing its buffered data to disk.
  static constexpr const char* kWriterFlushThresholdBytes =
      "writer_flush_threshold_bytes";

  /// If true, array_agg() aggregation function will ignore nulls in the input.
  static constexpr const char* kPrestoArrayAggIgnoreNulls =
      "presto.array_agg.ignore_nulls";

  /// If true, array_agg() aggregation function will ignore nulls in the input.
  static constexpr const char* kPrestoSetAggIgnoreNulls =
      "presto.set_agg.ignore_nulls";

  // The default number of expected items for the bloomfilter.
  static constexpr const char* kSparkBloomFilterExpectedNumItems =
      "spark.bloom_filter.expected_num_items";

  /// The default number of bits to use for the bloom filter.
  static constexpr const char* kSparkBloomFilterNumBits =
      "spark.bloom_filter.num_bits";

  /// The max number of bits to use for the bloom filter.
  static constexpr const char* kSparkBloomFilterMaxNumBits =
      "spark.bloom_filter.max_num_bits";

  // The policy to deduplicate map keys in builtin function: CreateMap,
  // MapFromArrays, MapFromEntries, StringToMap, MapConcat and TransformKeys.
  // When EXCEPTION, the query fails if duplicated map keys are detected. When
  // LAST_WIN, the map key that is inserted at last takes precedence.
  static constexpr const char* kSparkMapKeyDedupPolicy =
      "spark.mapKeyDedupPolicy";

  /// The current spark partition id.
  static constexpr const char* kSparkPartitionId = "spark.partition_id";

  /// The number of local parallel table writer operators per task.
  static constexpr const char* kTaskWriterCount = "task_writer_count";

  /// The number of local parallel table writer operators per task for
  /// partitioned writes. If not set, use "task_writer_count".
  static constexpr const char* kTaskPartitionedWriterCount =
      "task_partitioned_writer_count";

  /// If true, finish the hash probe on an empty build table for a specific set
  /// of hash joins.
  static constexpr const char* kHashProbeFinishEarlyOnEmptyBuild =
      "hash_probe_finish_early_on_empty_build";

  /// The minimum number of table rows that can trigger the parallel hash join
  /// table build.
  static constexpr const char* kMinTableRowsForParallelJoinBuild =
      "min_table_rows_for_parallel_join_build";

  static constexpr const char* kExchangeCompression = "exchange_compression";

  static constexpr const char* kNativeCacheEnabled = "native_cache_enabled";

  static constexpr const char* kMultiDriver = "multi_driver";

  static constexpr const char* kZeroBasedArrayIndex = "zero_based_array_index";

  /// If set to true, then during execution of tasks, the output vectors of
  /// every operator are validated for consistency. This is an expensive check
  /// so should only be used for debugging. It can help debug issues where
  /// malformed vector cause failures or crashes by helping identify which
  /// operator is generating them.
  static constexpr const char* kValidateOutputFromOperators =
      "debug.validate_output_from_operators";

  /// If true, enable caches in expression evaluation for performance, including
  /// ExecCtx::vectorPool_, ExecCtx::decodedVectorPool_,
  /// ExecCtx::selectivityVectorPool_, Expr::baseDictionary_,
  /// Expr::dictionaryCache_, and Expr::cachedDictionaryIndices_. Otherwise,
  /// disable the caches.
  static constexpr const char* kEnableExpressionEvaluationCache =
      "enable_expression_evaluation_cache";

  // For a given shared subexpression, the maximum distinct sets of inputs we
  // cache results for. Lambdas can call the same expression with different
  // inputs many times, causing the results we cache to explode in size. Putting
  // a limit contains the memory usage.
  static constexpr const char* kMaxSharedSubexprResultsCached =
      "max_shared_subexpr_results_cached";

  /// Maximum number of splits to preload. Set to 0 to disable preloading.
  static constexpr const char* kMaxSplitPreloadPerDriver =
      "max_split_preload_per_driver";

  static constexpr const char* kPreloadBytesLimit = "preload_bytes_limit";

  static constexpr const char* kPreloadAdaptive = "preload_adaptive_enabled";

  /// If not zero, specifies the cpu time slice limit in ms that a driver thread
  /// can continuously run without yielding. If it is zero, then there is no
  /// limit.
  static constexpr const char* kDriverCpuTimeSliceLimitMs =
      "driver_cpu_time_slice_limit_ms";

  /// Enable query tracing flag.
  static constexpr const char* kQueryTraceEnabled = "query_trace_enabled";

  /// Base dir of a query to store tracing data.
  static constexpr const char* kQueryTraceDir = "query_trace_dir";

  /// A comma-separated list of plan node ids whose input data will be traced.
  /// Empty string if only want to trace the query metadata.
  static constexpr const char* kQueryTraceNodeIds = "query_trace_node_ids";

  /// The max trace bytes limit. Tracing is disabled if zero.
  static constexpr const char* kQueryTraceMaxBytes = "query_trace_max_bytes";

  /// The regexp of traced task id. We only enable trace on a task if its id
  /// matches.
  static constexpr const char* kQueryTraceTaskRegExp =
      "query_trace_task_reg_exp";

  /// Config used to create operator trace directory. This config is provided to
  /// underlying file system and the config is free form. The form should be
  /// defined by the underlying file system.
  static constexpr const char* kOpTraceDirectoryCreateConfig =
      "op_trace_directory_create_config";

  static constexpr const char* kEnableEstimateRowSizeBasedOnSample =
      "enable_estimate_row_size_based_on_sample";

  static constexpr const char* kTimeParserPolicy =
      "spark.legacy_time_parser_policy";

  static constexpr const char* kThrowExceptionWhenEncounterBadJson =
      "throw_exception_when_encounter_bad_json";

  /// If a key is found in multiple given maps, by default that key's value in
  /// the resulting map comes from the last one of those maps. When true, throw
  /// exception on duplicate map key.
  static constexpr const char* kThrowExceptionOnDuplicateMapKeys =
      "throw_exception_on_duplicate_map_keys";

  static constexpr const char* kUseDOMParserInGetJsonObject =
      "use_dom_parser_in_get_json_object";

  static constexpr const char* kGetJsonObjectEscapeEmoji =
      "get_json_object_escape_emoji";
  static constexpr const char* kUseSonicJson = "use_sonic_json";

  static constexpr const char* kThrowExceptionWhenEncounterBadTimestamp =
      "throw_exception_when_encounter_bad_timestamp";

  static constexpr const char* kRegexMatchDanglingRightBrackets =
      "regex_match_dangling_right_brackets";

  static constexpr const char* kIgnoreCorruptFiles = "ignore_corrupt_files";

  static constexpr const char* kTaskMaxFailures = "task_max_failures";

  static constexpr const char* kCanBeTreatedAsCorruptedFileExceptions =
      "can_be_treated_as_corrupted_file_exceptions";

  static constexpr const char* KEnableAeolusFunction = "enable_aeolus_function";

  static constexpr const char* kMaxHashTableSize = "max_hashtable_size";

  // for presto true, default false
  static constexpr const char* kThrowExceptionWhenCastIntToTimestamp =
      "throw_exception_when_cast_int_to_timestamp";

  static constexpr const char* kUnnestWithOrdinalityFromZero =
      "unnest_with_ordinality_from_zero";

#ifdef ENABLE_BOLT_EXPR_JIT
  // LLVM JIT

  bool isBoltJITExprenabled() const {
    int32_t flag = get<int32_t>(kJitLevel, -1);
    return flag & 4;
  }
#endif

  static constexpr const char* kAbandonBuildNoDupHashMinRows =
      "abandon_build_no_dup_hash_min_rows";

  static constexpr const char* kAbandonBuildNoDupHashMinPct =
      "abandon_build_no_dup_hash_min_pct";

  static constexpr const char* kEnableSingleThreadedPartialExecution =
      "enable_single_threaded_partial_execution";

  std::string timeParserPolicy() const {
    return get<std::string>(kTimeParserPolicy, "legacy");
  }

  uint64_t queryMaxMemoryPerNode() const {
    return config::toCapacity(
        get<std::string>(kQueryMaxMemoryPerNode, "0B"),
        config::CapacityUnit::BYTE);
  }

  static constexpr const char* kDataRetentionUpdate =
      "data_retention_update_enabled";

  static constexpr const char* kDataRetentionShuffleBased =
      "data_retention_update_shuffle_based";

  static constexpr const char* kParquetRepDefMemoryLimit =
      "parquet_repdef_memory_limit";

  /**
   * LLVM JIT enabled
   * -1 : enable all jit (by default)
   * 0 : disable all jit
   * 1 : only enable jit for row cmp/= row
   * 2 : only enable jit for row = vectors
   * 4 : only enable jit for expression (disable by default)
   */
  static constexpr const char* kJitLevel = "jit.level";

  // expired, to deleted later
  static constexpr const char* kBoltJitEnabled = "bolt.jit.enabled";
  // For morsel-driven Bolt
  static constexpr const char* kEnableMorselDriven = "enable_morsel_driven";
  static constexpr const char* kMorselSize = "morsel_size";
  static constexpr const char* kMorselDrivenPrimedQueueSize =
      "morsel_driven_primed_queue_size";

  static constexpr const char* kEsBuildPlanCacheSize =
      "es.buildplan.cache.size";

  static constexpr const char* kEsBuildTaskCacheSize =
      "es.buildtask.cache.size";

  static constexpr const char* kEsStreamingThreadPoolSize =
      "es.streaming.task.threadpool.size";

  static constexpr const char* kEsStreamingTaskMaxDriver =
      "es.streaming.task.max.driver";

  static constexpr const char* kEsStreamingTaskBufferedBytes =
      "es.streaming.task.buffered.bytes";

  static constexpr const char* kEsStreamingTaskMaxWaitMS =
      "es.streaming.task.max.waitms";

  static constexpr const char* kEsDateTruncOptimization =
      "es.date_trunc.optimization";

  static constexpr const char* kEnableBoltSignalHandler =
      "es.enable.bolt.signalhandler";

  static constexpr const char* kReasonableBoltStackSize =
      "es.reasonable.bolt.stack.size";

  // ES queries use intermediate type as rawInputType while step is kFinal.
  static constexpr const char* kEsAggIntermediateTypeAsInputEnabled =
      "es.agg.intermediate_type_as_input.enabled";

  static constexpr const char* kLimitOffsetDictionaryEncoding =
      "limit.offset.dictionary.encoding";

  static constexpr const char* kSessionOwner = "session_owner";

  static constexpr const char* kSumAggOverflowCheck =
      "sumAgg_overflow_check.enabled";

  // Timestamp unit used during Bolt-Arrow conversion.
  static constexpr const char* kArrowBridgeTimestampUnit =
      "arrow_bridge_timestamp_unit";

  static constexpr const char* kHashJoinSkewedPartitionEnabled =
      "hash_join_skewed_partition_enabled";

  static constexpr const char* kSkewFileSizeRatioThreshold =
      "hash_join_skewed_file_size_ratio";

  static constexpr const char* kSkewRowCountRatioThreshold =
      "hash_join_skewed_row_count_ratio";

  // -1 means print all exceptions, usualing for debug
  // 0 means disable all exceptions,
  // 1 means print exceptions whose prefix is in the white list(default)
  // other value means the default value
  static constexpr const char* kExceptionTraceLevel = "exception_trace_level";

  // enable to print exceptions whose prefix is in the white list, default
  // "std::,folly::arrow::", each is a namespace of exceptioins.
  static constexpr const char* kExceptionTraceWhitelist =
      "exception_trace_whitelist";

  static constexpr const char* kColocateUDFEnabled = "colocate_udf_enabled";

  static constexpr const char* kColocateErrorRetries = "colocate_error_retries";

  static constexpr const char* kColocateRpcTimeout = "colocate_rpc_timeout";

  static constexpr const char* kAbtestMetrics = "abtest_metrics";

  static constexpr const char* kEnableOptimizedCast = "optimized.cast.enable";

  static constexpr const char* kEnableFlinkCompatible =
      "compatible.flink.enable";

  static constexpr const char* kFlinkJniUdfTaskInfo = "flink.jni.udf.task.info";

  static constexpr const char* kEnableSonicJsonSplit = "sonic.json_split";

  static constexpr const char* kEnableSonicJsonParse = "sonic.json_parse";

  static constexpr const char* kEnableSonicJsonToMap = "sonic.json_to_map";

  static constexpr const char* kEnableSonicIsJsonScalar =
      "sonic.is_json_scalar";

  static constexpr const char* kEnableSonicJSsonArrayContains =
      "sonic.json_array_contains";

  static constexpr const char* kEnableSonicJsonArrayLength =
      "sonic.json_array_length";

  static constexpr const char* kEnableSonicJsonExtractScalar =
      "sonic.json_extract_scalar";

  static constexpr const char* kEnableSonicJsonExtract = "sonic.json_extract";

  static constexpr const char* kEnableSonicJsonSize = "sonic.json_size";

  static constexpr const char* kLocalShufflePoolSize =
      "local_shuffle_pool_size";

  /// If this is true, then operators that evaluate expressions will track
  /// their stats and return them as part of their
  /// operator stats. Tracking these stats can be expensive (especially if
  /// operator stats are retrieved frequently) and this allows the user to
  /// explicitly enable it.
  static constexpr const char* kOperatorTrackExpressionStats =
      "operator_track_expression_stats";

  /// Enable filter pushdown for filters with map subscript operation
  static constexpr const char* kMapSubscriptFilterPushdown =
      "map_subscript_filter_pushdown";

  static constexpr const char* kDynamicConcurrencyAdjustmentEnabled =
      "dynamic_concurrency_adjustment_enabled";

  static constexpr const char* kBoltTaskSchedulingEnabled =
      "bolt_task_scheduling_enabled";

  // Lifetime of cached entities (key encryption keys, local wrapping keys, KMS
  // client objects).
  static constexpr const char* kCacheLifetimePropertyName =
      "parquet.encryption.cache.lifetime.seconds";

  // Decryption toggle
  static constexpr const char* kDecryptionEnabled =
      "parquet.encryption.decrypt.enabled";

  /// Priority of the query in the memory pool reclaimer. Lower value means
  /// higher priority. This is used in global arbitration victim selection.
  static constexpr const char* kQueryMemoryReclaimerPriority =
      "query_memory_reclaimer_priority";

  /// When set to true, statistical aggregate function returns Double.NaN if
  /// divide by zero occurred during expression evaluation, otherwise, it
  /// returns null.
  static constexpr const char* kSparkLegacyStatisticalAggregate =
      "spark_legacy_statistical_aggregate";

  bool operatorTrackExpressionStats() const {
    return get<bool>(kOperatorTrackExpressionStats, false);
  }

  uint64_t maxQueryMemory() const {
    static constexpr uint64_t kDefault = std::numeric_limits<int64_t>::max();
    return get<uint64_t>(kMaxMemoryPerQuery, kDefault);
  }

  uint64_t maxPartialAggregationMemoryUsage() const {
    static constexpr uint64_t kDefault = 1L << 24;
    return get<uint64_t>(kMaxPartialAggregationMemory, kDefault);
  }

  uint64_t maxExtendedPartialAggregationMemoryUsage() const {
    static constexpr uint64_t kDefault = 1L << 26;
    return get<uint64_t>(kMaxExtendedPartialAggregationMemory, kDefault);
  }

  int32_t abandonPartialAggregationMinRows() const {
    return get<int32_t>(kAbandonPartialAggregationMinRows, 100'000);
  }

  int32_t abandonPartialAggregationMinPct() const {
    return get<int32_t>(kAbandonPartialAggregationMinPct, 80);
  }

  int32_t abandonPartialAggregationMinFinalPct() const {
    return get<int32_t>(kAbandonPartialAggregationMinFinalPct, 75);
  }

  bool preferPartialAggregationSpill() const {
    return get<bool>(kPreferPartialAggregationSpill, false);
  }

  bool isHashAggregationCompositeOutputEnabled() const {
    return get<bool>(kHashAggregationCompositeOutputEnabled, false);
  }

  bool isUniqueRowOptimizationEnabled() const {
    return get<bool>(kHashAggregationUniqueRowOpt, false);
  }

  int32_t hashAggregationCompositeAccumulatorRatio() const {
    return get<int32_t>(kHashAggregationCompositeOutputAccumulatorRatio, 5);
  }

  uint64_t adaptiveSkippedDataSizeThreshold() const {
    return get<uint64_t>(kAdaptiveSkippedDataSizeThreshold, 20UL << 30);
  }

  int32_t partialAggregationSpillMaxPct() const {
    return get<int32_t>(kPartialAggregationSpillMaxPct, 50);
  }

  int32_t abandonPartialTopNRowNumberMinRows() const {
    return get<int32_t>(kAbandonPartialTopNRowNumberMinRows, 100'000);
  }

  int32_t abandonPartialTopNRowNumberMinPct() const {
    return get<int32_t>(kAbandonPartialTopNRowNumberMinPct, 80);
  }

  uint64_t aggregationSpillMemoryThreshold() const {
    static constexpr uint64_t kDefault = 0;
    return get<uint64_t>(kAggregationSpillMemoryThreshold, kDefault);
  }

  uint64_t joinSpillMemoryThreshold() const {
    static constexpr uint64_t kDefault = 0;
    return get<uint64_t>(kJoinSpillMemoryThreshold, kDefault);
  }

  uint64_t orderBySpillMemoryThreshold() const {
    static constexpr uint64_t kDefault = 0;
    return get<uint64_t>(kOrderBySpillMemoryThreshold, kDefault);
  }

  uint64_t spillLowCompressByteThreshold() const {
    static constexpr uint64_t kDefault = 4UL << 30;
    return get<uint64_t>(kSpillLowCompressByteThreshold, kDefault);
  }

  uint64_t spillHighCompressByteThreshold() const {
    static constexpr uint64_t kDefault = 20UL << 30;
    return get<uint64_t>(kSpillHighCompressByteThreshold, kDefault);
  }

  uint64_t maxSpillRunRows() const {
    static constexpr uint64_t kDefault = 12UL << 20;
    return get<uint64_t>(kMaxSpillRunRows, kDefault);
  }

  uint64_t maxSpillBytes() const {
#ifdef SPARK_COMPATIBLE
    static constexpr uint64_t kDefault = 0UL;
#else
    static constexpr uint64_t kDefault = 100UL << 30;
#endif
    return get<uint64_t>(kMaxSpillBytes, kDefault);
  }

  /// Returns the maximum number of bytes to buffer in PartitionedOutput
  /// operator to avoid creating tiny SerializedPages.
  ///
  /// For PartitionedOutputNode::Kind::kPartitioned, PartitionedOutput operator
  /// would buffer up to that number of bytes / number of destinations for each
  /// destination before producing a SerializedPage.
  uint64_t maxPartitionedOutputBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxPartitionedOutputBufferSize, kDefault);
  }

  /// Returns the maximum size in bytes for the task's buffered output.
  ///
  /// The producer Drivers are blocked when the buffered size exceeds
  /// this. The Drivers are resumed when the buffered size goes below
  /// OutputBufferManager::kContinuePct % of this.
  uint64_t maxOutputBufferSize() const {
    return get<uint64_t>(kMaxOutputBufferSize, maxArbitraryBufferSize());
  }

  /// Deprecated. Use maxBufferSize() instead.
  uint64_t maxArbitraryBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxArbitraryBufferSize, kDefault);
  }

  uint64_t maxLocalExchangeBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxLocalExchangeBufferSize, kDefault);
  }

  uint64_t maxExchangeBufferSize() const {
    static constexpr uint64_t kDefault = 32UL << 20;
    return get<uint64_t>(kMaxExchangeBufferSize, kDefault);
  }

  uint64_t maxMergeExchangeBufferSize() const {
    static constexpr uint64_t kDefault = 128UL << 20;
    return get<uint64_t>(kMaxMergeExchangeBufferSize, kDefault);
  }

  uint64_t preferredOutputBatchBytes() const {
    static constexpr uint64_t kDefault = 10UL << 20;
    return get<uint64_t>(kPreferredOutputBatchBytes, kDefault);
  }

  uint32_t preferredOutputBatchRows() const {
    return get<uint32_t>(kPreferredOutputBatchRows, 1024);
  }

  uint32_t maxOutputBatchRows() const {
    return get<uint32_t>(kMaxOutputBatchRows, 10'000);
  }

  uint32_t minOutputBatchRows() const {
    return get<uint32_t>(kMinOutputBatchRows, 1);
  }

  uint32_t tableScanGetOutputTimeLimitMs() const {
    return get<uint64_t>(kTableScanGetOutputTimeLimitMs, 5'000);
  }

  bool hashAdaptivityEnabled() const {
    return get<bool>(kHashAdaptivityEnabled, true);
  }

  uint32_t writeStrideSize() const {
    static constexpr uint32_t kDefault = 100'000;
    return kDefault;
  }

  bool flushPerBatch() const {
    static constexpr bool kDefault = true;
    return kDefault;
  }

  bool adaptiveFilterReorderingEnabled() const {
    return get<bool>(kAdaptiveFilterReorderingEnabled, true);
  }

  bool isLegacyCast() const {
    return get<bool>(kLegacyCast, false);
  }

  std::string isSparkLegacyCastComplexTypesToStringEnabled() const {
    return get<std::string>(
        kSparkLegacyCastComplexTypesToStringEnabled, "false");
  }

  bool isMatchStructByName() const {
    return get<bool>(kCastMatchStructByName, false);
  }

  bool isCastToIntByTruncate() const {
    return get<bool>(kCastToIntByTruncate, false);
  }

  std::string specTimeZone() const {
    return get<std::string>(kSpecTimezone, "America/Los_Angeles");
  }

  bool isIso8601() const {
    return get<bool>(kCastStringToDateIsIso8601, true);
  }

  bool codegenEnabled() const {
    return get<bool>(kCodegenEnabled, false);
  }

  std::string codegenConfigurationFilePath() const {
    return get<std::string>(kCodegenConfigurationFilePath, "");
  }

  bool codegenLazyLoading() const {
    return get<bool>(kCodegenLazyLoading, true);
  }

  bool adjustTimestampToTimezone() const {
    return get<bool>(kAdjustTimestampToTimezone, false);
  }

  std::string sessionTimezone() const {
    return get<std::string>(kSessionTimezone, "");
  }

  bool exprEvalSimplified() const {
    return get<bool>(kExprEvalSimplified, false);
  }

  /// Returns true if spilling is enabled.
  bool spillEnabled() const {
    return get<bool>(kSpillEnabled, false);
  }

  config::AB_MODE spillUringEnabled() const {
#ifdef IO_URING_SUPPORTED
    if (config::checkKernelVersion(5, 1, 0)) {
      return config::STR_TO_AB_MODE(
          get<std::string>(kSpillUringEnabled, "false"));
    } else {
      LOG(ERROR)
          << "Runtime Linux kernel version is older than 5.1. IO_URING is disabled.";
      return config::OFF;
    }
#else
    return config::OFF;
#endif
  }

  /// Returns 'is aggregation spilling enabled' flag. Must also check the
  /// spillEnabled()!
  bool aggregationSpillEnabled() const {
    return get<bool>(kAggregationSpillEnabled, true);
  }

  /// Returns 'is hll sketch return rounded result' flag.
  bool hllSketchRounded() const {
#ifdef ES_COMPATIBLE
    return get<bool>(kHllSketchRounded, true);
#else
    return get<bool>(kHllSketchRounded, false);
#endif
  }

  double spilledAggregationBypassHTRatio() const {
    double ratio = get<double>(kSpilledAggregationBypassHTRatio, 0.95);
    if (ratio < 0) {
      ratio = 0;
    } else if (ratio > 1) {
      ratio = 1.1; // avoid overflow
    }
    return ratio;
  }

  /// Returns 'is join spilling enabled' flag. Must also check the
  /// spillEnabled()!
  bool joinSpillEnabled() const {
    return get<bool>(kJoinSpillEnabled, true);
  }

  /// Returns 'is orderby spilling enabled' flag. Must also check the
  /// spillEnabled()!
  bool orderBySpillEnabled() const {
    return get<bool>(kOrderBySpillEnabled, true);
  }

  bool orderBySpillInOutputStageEnabled() const {
    return get<bool>(kOrderBySpillInOutputStageEnabled, true);
  }

  /// Returns true if spilling is enabled for Window operator. Must also
  /// check the spillEnabled()!
  bool windowSpillEnabled() const {
    return get<bool>(kWindowSpillEnabled, true);
  }

  /// Returns 'is writer spilling enabled' flag. Must also check the
  /// spillEnabled()!
  bool writerSpillEnabled() const {
    return get<bool>(kWriterSpillEnabled, true);
  }

  /// Returns true if spilling is enabled for RowNumber operator. Must also
  /// check the spillEnabled()!
  bool rowNumberSpillEnabled() const {
    return get<bool>(kRowNumberSpillEnabled, true);
  }

  /// Returns true if spilling is enabled for TopNRowNumber operator. Must also
  /// check the spillEnabled()!
  bool topNRowNumberSpillEnabled() const {
    return get<bool>(kTopNRowNumberSpillEnabled, true);
  }

  /// Return 'export to string view enabled' flag.
  bool exportToStringViewEnabled() const {
    return get<bool>(kExportToStringViewEnabled, false);
  }

  /// Return 'flattenDictionary enabled' flag used for determining which
  /// strategy to use for ES.
  bool exportFlattenDictionaryEnabled() const {
    return get<bool>(kExportFlattenDictionaryEnabled, true);
  }

  /// Return 'flattenConstant enabled' flag used for determining which strategy
  /// to use for ES.
  bool exportFlattenConstantEnabled() const {
    return get<bool>(kExportFlattenConstantEnabled, true);
  }

  // Returns a percentage of aggregation or join input batches that
  // will be forced to spill for testing. 0 means no extra spilling.
  int32_t testingSpillPct() const {
    return get<int32_t>(kTestingSpillPct, 0);
  }

  int32_t maxSpillLevel() const {
    return get<int32_t>(kMaxSpillLevel, 4);
  }

  /// Default offset spill start partition bit. It is used with
  /// 'kSpillNumPartitionBits' together to
  /// calculate the spilling partition number for join spill or aggregation
  /// spill.
  uint8_t spillStartPartitionBit() const {
    constexpr uint8_t kDefaultStartBit = 48;
    return get<uint8_t>(kSpillStartPartitionBit, kDefaultStartBit);
  }

  // control row based spill, "raw" means enable row based spill with raw
  // data, "compression" means enable row based spill with compressed data,
  // otherwise disable row based spill
  std::string rowBasedSpillMode() const {
    return get<std::string>(kRowBasedSpillMode, kDefaultRowBasedSpillMode);
  }

  /// Returns the vector serde kind used for single partition spill.
  /// Options are:
  ///   "Presto"      : use PrestoVectorSerde
  ///   "UnsafeRow"   : use UnsafeRowVectorSerde
  ///   "CompactRow"  : use CompactRowVectorSerde
  ///   "Arrow"       : use ArrowVectorSerde
  ///   "" (default)  : use system default serde (PrestoVectorSerde as of now)
  std::string singlePartitionSpillSerdeKind() const {
    return get<std::string>(kSinglePartitionSpillSerdeKind, "");
  }

  /// Returns the timestamp unit used in Bolt-Arrow conversion.
  /// 0: second, 3: milli, 6: micro, 9: nano.
  uint8_t arrowBridgeTimestampUnit() const {
    constexpr uint8_t kDefaultUnit = 9;
    return get<uint8_t>(kArrowBridgeTimestampUnit, kDefaultUnit);
  }

  uint8_t spillNumPartitionBits() const {
    constexpr uint8_t kDefaultBits = 3;
    constexpr uint8_t kMaxBits = 3;
    return std::min(
        kMaxBits, get<uint8_t>(kSpillNumPartitionBits, kDefaultBits));
  }

  uint64_t writerFlushThresholdBytes() const {
    return get<uint64_t>(kWriterFlushThresholdBytes, 96L << 20);
  }

  uint64_t maxSpillFileSize() const {
    constexpr uint64_t kDefaultMaxFileSize = 0;
    return get<uint64_t>(kMaxSpillFileSize, kDefaultMaxFileSize);
  }

  uint64_t minSpillRunSize() const {
    constexpr uint64_t kDefaultMinSpillRunSize = 256 << 20; // 256MB.
    return get<uint64_t>(kMinSpillRunSize, kDefaultMinSpillRunSize);
  }

  std::string spillCompressionKind() const {
    return get<std::string>(kSpillCompressionKind, "none");
  }

  uint64_t spillWriteBufferSize() const {
    // The default write buffer size set to 1MB.
    return get<uint64_t>(kSpillWriteBufferSize, 1L << 20);
  }

  std::string spillFileCreateConfig() const {
    return get<std::string>(kSpillFileCreateConfig, "");
  }

  int32_t abandonBuildNoDupHashMinRows() const {
    return get<int32_t>(kAbandonBuildNoDupHashMinRows, 100'000);
  }

  int32_t abandonBuildNoDupHashMinPct() const {
    return get<int32_t>(kAbandonBuildNoDupHashMinPct, 80);
  }

  /// Returns the minimal available spillable memory reservation in percentage
  /// of the current memory usage. Suppose the current memory usage size of M,
  /// available memory reservation size of N and min reservation percentage of
  /// P, if M * P / 100 > N, then spiller operator needs to grow the memory
  /// reservation with percentage of spillableReservationGrowthPct(). This
  /// ensures we have sufficient amount of memory reservation to process the
  /// large input outlier.
  int32_t minSpillableReservationPct() const {
    constexpr int32_t kDefaultPct = 5;
    return get<int32_t>(kMinSpillableReservationPct, kDefaultPct);
  }

  /// Returns the spillable memory reservation growth percentage of the previous
  /// memory reservation size. 10 means exponential growth along a series of
  /// integer powers of 11/10. The reservation grows by this much until it no
  /// longer can, after which it starts spilling.
  int32_t spillableReservationGrowthPct() const {
    constexpr int32_t kDefaultPct = 10;
    return get<int32_t>(kSpillableReservationGrowthPct, kDefaultPct);
  }

  bool queryTraceEnabled() const {
    return get<bool>(kQueryTraceEnabled, false);
  }

  std::string queryTraceDir() const {
    // The default query trace dir, empty by default.
    return get<std::string>(kQueryTraceDir, "");
  }

  std::string queryTraceNodeIds() const {
    // The default query trace nodes, empty by default.
    return get<std::string>(kQueryTraceNodeIds, "");
  }

  uint64_t queryTraceMaxBytes() const {
    return get<uint64_t>(kQueryTraceMaxBytes, 0);
  }

  std::string queryTraceTaskRegExp() const {
    // The default query trace task regexp, empty by default.
    return get<std::string>(kQueryTraceTaskRegExp, "");
  }

  std::string opTraceDirectoryCreateConfig() const {
    return get<std::string>(kOpTraceDirectoryCreateConfig, "");
  }

  bool prestoArrayAggIgnoreNulls() const {
    return get<bool>(kPrestoArrayAggIgnoreNulls, false);
  }

  bool prestoSetAggIgnoreNulls() const {
    return get<bool>(kPrestoSetAggIgnoreNulls, false);
  }

  int64_t sparkBloomFilterExpectedNumItems() const {
    constexpr int64_t kDefault = 1'000'000L;
    return get<int64_t>(kSparkBloomFilterExpectedNumItems, kDefault);
  }

  int64_t sparkBloomFilterNumBits() const {
    constexpr int64_t kDefault = 8'388'608L;
    return get<int64_t>(kSparkBloomFilterNumBits, kDefault);
  }

  // Spark kMaxNumBits is 67'108'864, but bolt has memory limit sizeClassSizes
  // 256, so decrease it to not over memory limit.
  int64_t sparkBloomFilterMaxNumBits() const {
    constexpr int64_t kDefault = 4'096 * 1024;
    auto value = get<int64_t>(kSparkBloomFilterMaxNumBits, kDefault);
    BOLT_USER_CHECK_LE(
        value,
        kDefault,
        "{} cannot exceed the default value",
        kSparkBloomFilterMaxNumBits);
    return value;
  }

  std::string sparkMapKeyDedupPolicy() const {
    std::string res = get<std::string>(kSparkMapKeyDedupPolicy, "EXCEPTION");
    for (auto& c : res) {
      c = std::toupper(static_cast<unsigned char>(c));
    }
    return res;
  }

  int32_t sparkPartitionId() const {
    auto id = get<int32_t>(kSparkPartitionId);
    BOLT_CHECK(id.has_value(), "Spark partition id is not set.");
    auto value = id.value();
    BOLT_CHECK_GE(value, 0, "Invalid Spark partition id.");
    return value;
  }

  size_t planNodeCacheSize() const {
    constexpr size_t kDefaultSize = 0;
    return get<size_t>(kEsBuildPlanCacheSize, kDefaultSize);
  }

  size_t taskCfgCacheSize() const {
    constexpr size_t kDefaultSize = 10;
    return get<size_t>(kEsBuildTaskCacheSize, kDefaultSize);
  }

  size_t esStreamingThreadPoolSize() const {
    const size_t kDefaultThreadPoolSize = std::thread::hardware_concurrency();
    return get<size_t>(kEsStreamingThreadPoolSize, kDefaultThreadPoolSize);
  }

  uint32_t esStreamingTaskMaxDrivers() const {
    constexpr uint32_t kDefaultMaxDrivers = 1;
    return get<uint32_t>(kEsStreamingTaskMaxDriver, kDefaultMaxDrivers);
  }

  uint64_t esStreamingTaskBufferedBytes() const {
    constexpr uint64_t kDefaultBufferedBytes = 1048576; // 1MB
    return get<uint64_t>(kEsStreamingTaskBufferedBytes, kDefaultBufferedBytes);
  }

  uint64_t esStreamingTaskMaxWaitMS() const {
    constexpr uint64_t kDefaultTaskWaitMS = 10'000'000; // 1 second
    return get<uint64_t>(kEsStreamingTaskMaxWaitMS, kDefaultTaskWaitMS);
  }

  bool esDateTruncOptimization() const {
    return get<bool>(kEsDateTruncOptimization, true);
  }

  bool exprTrackCpuUsage() const {
    return get<bool>(kExprTrackCpuUsage, false);
  }

  uint64_t reasonableBoltThreadSize() const {
    constexpr uint64_t kReasonableSize = 10;
    return get<uint64_t>(kReasonableBoltStackSize, kReasonableSize);
  }

  bool esAggIntermediateTypeAsInputEnabled() const {
    return get<bool>(kEsAggIntermediateTypeAsInputEnabled, false);
  }

  bool enableBoltSignalHandler() const {
    return get<bool>(kEnableBoltSignalHandler, false);
  }

  bool operatorTrackCpuUsage() const {
    return get<bool>(kOperatorTrackCpuUsage, true);
  }

  uint32_t taskWriterCount() const {
    return get<uint32_t>(kTaskWriterCount, 4);
  }

  uint32_t taskPartitionedWriterCount() const {
    return get<uint32_t>(kTaskPartitionedWriterCount)
        .value_or(taskWriterCount());
  }

  bool hashProbeFinishEarlyOnEmptyBuild() const {
    return get<bool>(kHashProbeFinishEarlyOnEmptyBuild, true);
  }

  uint32_t minTableRowsForParallelJoinBuild() const {
    return get<uint32_t>(kMinTableRowsForParallelJoinBuild, 1'000);
  }

  bool isExchangeCompressionEnabled() const {
    return get<bool>(kExchangeCompression, false);
  }

  bool isNativeCacheEnabled() const {
    return get<bool>(kNativeCacheEnabled, true);
  }

  bool isMultiDriverEnabled() const {
    return get<bool>(kMultiDriver, false);
  }

  bool zeroBasedArrayIndex() const {
    return get<bool>(kZeroBasedArrayIndex, false);
  }

  bool validateOutputFromOperators() const {
    return get<bool>(kValidateOutputFromOperators, false);
  }

  bool isExpressionEvaluationCacheEnabled() const {
    return get<bool>(kEnableExpressionEvaluationCache, true);
  }

  uint32_t maxSharedSubexprResultsCached() const {
    // 10 was chosen as a default as there are cases where a shared
    // subexpression can be called in 2 different places and a particular
    // argument may be peeled in one and not peeled in another. 10 is large
    // enough to handle this happening for a few arguments in different
    // combinations.
    //
    // For example, when the UDF at the root of a shared subexpression does not
    // have default null behavior and takes an input that is dictionary encoded
    // with nulls set in the DictionaryVector. That dictionary
    // encoding may be peeled depending on whether or not there is a UDF above
    // it in the expression tree that has default null behavior and takes the
    // same input as an argument.
    return get<uint32_t>(kMaxSharedSubexprResultsCached, 10);
  }

  int32_t maxSplitPreloadPerDriver() const {
    return get<int32_t>(kMaxSplitPreloadPerDriver, 2);
  }

  int64_t preloadBytesLimit() const {
    return get<int64_t>(kPreloadBytesLimit, (1ULL << 30));
  }

  bool adaptivePreloadEnabled() const {
    return get<bool>(kPreloadAdaptive, true);
  }

  uint32_t driverCpuTimeSliceLimitMs() const {
    return get<uint32_t>(kDriverCpuTimeSliceLimitMs, 0);
  }

  bool iskEstimateRowSizeBasedOnSampleEnabled() const {
    return get<bool>(kEnableEstimateRowSizeBasedOnSample, false);
  }

  bool throwExceptionWhenEncounterBadJson() const {
    return get<bool>(kThrowExceptionWhenEncounterBadJson, false);
  }

  bool useDOMParserInGetJsonObject() const {
    return get<bool>(kUseDOMParserInGetJsonObject, false);
  }

  bool getJsonObjectEscapeEmoji() const {
    return get<bool>(kGetJsonObjectEscapeEmoji, true);
  }

  bool useSonicJson() const {
    return get<bool>(kUseSonicJson, true);
  }

  bool throwExceptionWhenEncounterBadTimestamp() const {
    return get<bool>(kThrowExceptionWhenEncounterBadTimestamp, false);
  }

  bool regexMatchDanglingRightBrackets() const {
    return get<bool>(kRegexMatchDanglingRightBrackets, true);
  }

  bool ignoreCorruptFiles() const {
    return get<bool>(kIgnoreCorruptFiles, false);
  }

  int64_t taskMaxFailures() const {
    return get<int64_t>(kTaskMaxFailures, 8);
  }

  std::string canBeTreatedAsCorruptedFileExceptions() const {
    return get<std::string>(kCanBeTreatedAsCorruptedFileExceptions, "");
  }

  bool isEnableAeolusFunction() const {
    return get<bool>(KEnableAeolusFunction, false);
  }

  bool throwExceptionWhenCastIntToTimestamp() const {
    return get<bool>(kThrowExceptionWhenCastIntToTimestamp, false);
  }

  bool isDataRetentionUpdateEnabled() const {
    return get<bool>(kDataRetentionUpdate, false);
  }

  bool isDataRetentionShuffleBased() const {
    return get<bool>(kDataRetentionShuffleBased, false);
  }

  std::string sessionOwner() const {
    return get<std::string>(kSessionOwner, "");
  }

  bool sumAggOverflowCheck() const {
    return get<bool>(kSumAggOverflowCheck, true);
  }

  bool limitOffsetDictionaryEncoding() const {
    return get<bool>(kLimitOffsetDictionaryEncoding, false);
  }

  int32_t maxHashTableSize() const {
    // limit max hash table bucket size
    // to reduce the performance impact caused by hash collisions
    // in high-order bits
    static constexpr uint64_t kDefault = 50L << 20;
    return get<int32_t>(kMaxHashTableSize, kDefault);
  }

  bool isSingleThreadedPartialExecutionEnabled() const {
    return get<bool>(kEnableSingleThreadedPartialExecution, false);
  }

  bool enableJitRowEqVectors() const {
    int32_t flag = get<int32_t>(kJitLevel, -1);
    return flag & 2;
  }

  // including row < row, row = row, row cmp row
  bool enableJitRowCmpRow() const {
    int32_t flag = get<int32_t>(kJitLevel, -1);
    return flag & 1;
  }

  int exceptionTraceLevel() const {
    return get<int>(kExceptionTraceLevel, 1);
  }

  std::string exceptionTraceWhitelist() const {
    return get<std::string>(kExceptionTraceWhitelist, "std::");
  }

  bool unnestWithOrdinalityFromZero() const {
    return get<bool>(kUnnestWithOrdinalityFromZero, false);
  }

  bool isHashJoinSkewedPartitionEnabled() const {
    return get<bool>(kHashJoinSkewedPartitionEnabled, true);
  }

  int32_t skewFileSizeRatioThreshold() const {
    return get<int32_t>(kSkewFileSizeRatioThreshold, 10);
  }

  int32_t skewRowCountRatioThreshold() const {
    return get<int32_t>(kSkewRowCountRatioThreshold, 100);
  }

  bool morselDrivenEnabled() const {
    return get<bool>(kEnableMorselDriven, false);
  }

  int32_t morselSize() const {
    return get<int32_t>(kMorselSize, 8192);
  }

  int32_t morselDrivenPrimedQueueSize() const {
    return get<int32_t>(kMorselDrivenPrimedQueueSize, 50);
  }

  bool colocateUDFEnabled() const {
    return get<bool>(kColocateUDFEnabled, true);
  }

  uint32_t colocateErrorRetries() const {
    return get<uint32_t>(kColocateErrorRetries, 5);
  }

  std::chrono::duration<double> colocateRpcTimeout() const {
    return config::toDuration(get<std::string>(kColocateRpcTimeout, "30s"));
  }

  // metrics used for abtesting splitted by comma, e.g., "metric1,metric2"
  std::unordered_set<std::string> getAbtestMetrics() const {
    return config::splitToStrSet(get<std::string>(kAbtestMetrics, ""));
  }

  bool enableOptimizedCast() const {
    return get<bool>(kEnableOptimizedCast, true);
  }

  bool enableFlinkCompatible() const {
    return get<bool>(kEnableFlinkCompatible, false);
  }

  std::string getFlinkJniUdfTaskInfo() const {
    return get<std::string>(kFlinkJniUdfTaskInfo, "");
  }

  bool enableSonicJsonSplit() const {
    return get<bool>(kEnableSonicJsonSplit, true);
  }

  bool enableSonicJsonParse() const {
    return get<bool>(kEnableSonicJsonParse, true);
  }

  bool enableSonicJsonToMap() const {
    return get<bool>(kEnableSonicJsonToMap, true);
  }

  bool enableSonicIsJsonScalar() const {
    return get<bool>(kEnableSonicIsJsonScalar, true);
  }

  bool enableSonicJsonArrayContains() const {
    return get<bool>(kEnableSonicJSsonArrayContains, true);
  }

  bool enableSonicJsonArrayLength() const {
    return get<bool>(kEnableSonicJsonArrayLength, true);
  }

  bool enableSonicJsonExtractScalar() const {
    return get<bool>(kEnableSonicJsonExtractScalar, true);
  }

  bool enableSonicJsonExtract() const {
    return get<bool>(kEnableSonicJsonExtract, true);
  }

  bool enableSonicJsonSize() const {
    return get<bool>(kEnableSonicJsonSize, true);
  }

  int64_t localShufflePoolSize() const {
    return get<int64_t>(kLocalShufflePoolSize, 1024);
  }

  int32_t parquetRepDefMemoryLimit() const {
    return get<int32_t>(kParquetRepDefMemoryLimit, 128UL << 20);
  }

  bool enableDynamicConcurrencyAdjustment() const {
    return get<bool>(kDynamicConcurrencyAdjustmentEnabled, false);
  }

  bool enableBoltTaskScheduling() const {
    return get<bool>(kBoltTaskSchedulingEnabled, false);
  }

  bool throwExceptionOnDuplicateMapKeys() const {
    return get<bool>(kThrowExceptionOnDuplicateMapKeys, false);
  }

  template <typename T>
  T get(const std::string& key, const T& defaultValue) const {
    return config_->get<T>(key, defaultValue);
  }
  template <typename T>
  std::optional<T> get(const std::string& key) const {
    return std::optional<T>(config_->get<T>(key));
  }

  /// Returns 'map subscript pushdown enable' flag.
  /// Map subscript filter pushdown is enabled by default.
  bool mapSubscriptFilterPushdownEnabled() const {
    return get<bool>(kMapSubscriptFilterPushdown, false);
  }

  int32_t queryMemoryReclaimerPriority() const {
    return get<int32_t>(
        kQueryMemoryReclaimerPriority, std::numeric_limits<int32_t>::max());
  }

  bool sparkLegacyStatisticalAggregate() const {
    return get<bool>(kSparkLegacyStatisticalAggregate, false);
  }

  /// Test-only method to override the current query config properties.
  /// It is not thread safe.
  void testingOverrideConfigUnsafe(
      std::unordered_map<std::string, std::string>&& values);

  std::unordered_map<std::string, std::string> rawConfigsCopy() const;

  double cacheLifeTimeSeconds() const {
    return get<double>(kCacheLifetimePropertyName, 600);
  }

  bool isDecryptionEnabled() const {
    return get<bool>(kDecryptionEnabled, false);
  }

 private:
  std::unique_ptr<bolt::config::ConfigBase> config_;
};
} // namespace bytedance::bolt::core
