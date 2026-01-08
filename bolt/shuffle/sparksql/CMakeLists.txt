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
# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

add_library(
  bolt_shuffle_spark_impl
    ShuffleColumnarToRowConverter.cpp
    ShuffleRowToColumnarConverter.cpp
    BoltShuffleReader.cpp
    BoltShuffleWriter.cpp
    BoltShuffleWriterV2.cpp
    BoltRowBasedSortShuffleWriter.cpp
    ShuffleWriterNode.cpp
    ShuffleReaderNode.cpp
    BoltArrowMemoryPool.cpp
    partitioner/FallbackRangePartitioner.cpp
    partitioner/HashPartitioner.cpp
    partitioner/RoundRobinPartitioner.cpp
    partitioner/SinglePartitioner.cpp
    partitioner/Partitioner.cpp
    partitioner/Partitioning.cpp
    partition_writer/PartitionWriter.cpp
    partition_writer/LocalPartitionWriter.cpp
    partition_writer/rss/RemotePartitionWriter.cpp
    partition_writer/rss/CelebornPartitionWriter.cpp
    partition_writer/rss/NativeCelebornClient.cpp
    CelebornReaderStreamIterator.cpp
    compression/Compression.cpp
    Payload.cpp
    ShuffleMemoryPool.cpp
    Spill.cpp
    Utils.cpp)

find_package(celeborn-cpp-client CONFIG REQUIRED)

target_link_libraries(
  bolt_shuffle_spark_impl
  PUBLIC
    arrow::arrow
    ryu::ryu
    bolt_vector
    bolt_core
    bolt_exec
    celeborn-cpp-client::celeborn-cpp-client
)

if(ENABLE_QAT)
  include(BuildQATzip)
  include(BuildQATZstd)
  target_sources(bolt_shuffle_spark_impl PRIVATE compression/qat/QatCodec.cpp)
  target_include_directories(bolt_shuffle_spark_impl PUBLIC ${QATZIP_INCLUDE_DIR} ${QATZSTD_INCLUDE_DIR})
  target_link_libraries(bolt_shuffle_spark_impl PUBLIC qatzip::qatzip qatzstd::qatzstd)
endif()

if(ENABLE_IAA)
  include(BuildQpl)
  target_include_directories(bolt_shuffle_spark_impl PUBLIC ${QPL_INCLUDE_DIR})
  target_sources(bolt_shuffle_spark_impl PRIVATE compression/qpl/qpl_job_pool.cpp compression/qpl/qpl_codec.cpp)
  target_link_libraries(bolt_shuffle_spark_impl PUBLIC qpl::qpl)
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
