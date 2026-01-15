# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2025-11-11.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------

bolt_add_library(
  bolt_exec
  $<$<BOOL:${ENABLE_META_SORT}>:meta/MetaRowSorter.cpp>
  AddressableNonNullValueList.cpp
  Aggregate.cpp
  AggregateCompanionAdapter.cpp
  AggregateCompanionSignatures.cpp
  AggregateFunctionRegistry.cpp
  AggregateInfo.cpp
  AggregateWindow.cpp
  AggregationMasks.cpp
  ArrowStream.cpp
  AssignUniqueId.cpp
  ContainerRow2RowSerde.cpp
  ContainerRowSerde.cpp
  DistinctAggregations.cpp
  Driver.cpp
  EnforceSingleRow.cpp
  Exchange.cpp
  ExchangeClient.cpp
  ExchangeQueue.cpp
  ExchangeSource.cpp
  ExecutorTaskScheduler.cpp
  Expand.cpp
  FilterProject.cpp
  Generator.cpp
  GroupId.cpp
  GroupingSet.cpp
  HashAggregation.cpp
  HashBuild.cpp
  HashJoinBridge.cpp
  HashPartitionFunction.cpp
  HashProbe.cpp
  HashTable.cpp
  IndexLookupJoin.cpp
  JoinBridge.cpp
  Limit.cpp
  LocalPartition.cpp
  LocalPlanner.cpp
  LocalShuffle.cpp
  MarkDistinct.cpp
  MemoryReclaimer.cpp
  Merge.cpp
  MergeJoin.cpp
  MergeSource.cpp
  NestedLoopJoinBuild.cpp
  NestedLoopJoinProbe.cpp
  Operator.cpp
  OperatorTraceReader.cpp
  OperatorTraceScan.cpp
  OperatorTraceWriter.cpp
  OperatorUtils.cpp
  OrderBy.cpp
  OutputBuffer.cpp
  OutputBufferManager.cpp
  PartitionedOutput.cpp
  PartitionFunction.cpp
  PlanNodeStats.cpp
  ProbeOperatorState.cpp
  PushBasedEvent.cpp
  RowContainer.cpp
  RowNumber.cpp
  RowsStreamingWindowBuild.cpp
  RowsStreamingWindowPartition.cpp
  SortBuffer.cpp
  SortedAggregations.cpp
  SortWindowBuild.cpp
  Spill.cpp
  SpillableWindowBuild.cpp
  Spiller.cpp
  SpillFile.cpp
  SpillOperatorGroup.cpp
  StreamingAggregation.cpp
  StreamingWindowBuild.cpp
  StreamingWindowBuild.cpp
  Strings.cpp
  TableScan.cpp
  TableWriteMerge.cpp
  TableWriter.cpp
  Task.cpp
  TaskTraceReader.cpp
  TaskTraceWriter.cpp
  TopN.cpp
  TopNRowNumber.cpp
  Trace.cpp
  TraceConfig.cpp
  TraceUtil.cpp
  Unnest.cpp
  Values.cpp
  VectorHasher.cpp
  Window.cpp
  WindowBuild.cpp
  WindowFunction.cpp
  WindowPartition.cpp
)

target_compile_features(bolt_exec PRIVATE cxx_std_20)

set_target_properties(bolt_exec PROPERTIES ENABLE_EXPORTS ON)

if(${ENABLE_BOLT_JIT})
  find_package(LLVM REQUIRED CONFIG)
endif()

target_link_libraries(
  bolt_exec
  bolt_filter
  bolt_file
  bolt_core
  bolt_vector
  bolt_connector
  bolt_time
  bolt_common_base
  bolt_arrow_bridge
  bolt_common_compression
  bolt_functions_json
  bolt_expression
  bolt_presto_serializer
  bolt_cpu_usage
  $<TARGET_NAME_IF_EXISTS:bolt_thrustjit>
  gfx::timsort
  arrow::arrow
)

if(${BOLT_ENABLE_TORCH})
  target_compile_definitions(bolt_exec PRIVATE BOLT_HAS_TORCH=1)
  target_include_directories(bolt_exec SYSTEM PRIVATE ${TORCH_INCLUDE_DIRS})
  target_link_libraries(bolt_exec bolt_torch)
endif()

if(${BOLT_BUILD_PYTHON_PACKAGE})
  target_compile_definitions(bolt_exec PRIVATE BOLT_HAS_PYTHON=1)
  target_include_directories(bolt_exec SYSTEM PRIVATE ${pybind11_INCLUDE_DIRS})
  target_link_libraries(bolt_exec bolt_python)
endif()

if(${BOLT_ENABLE_CUDF})
  target_compile_definitions(bolt_exec PRIVATE BOLT_HAS_CUDF=1)
  target_include_directories(bolt_exec SYSTEM PRIVATE ${cudf_INCLUDE_DIRS})
  target_link_libraries(bolt_exec bolt_cudf_exec cudf::cudf)
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(fuzzer)
  add_subdirectory(tests)
elseif(${BOLT_BUILD_TEST_UTILS})
  add_subdirectory(tests/utils)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()

add_subdirectory(prefixsort)
