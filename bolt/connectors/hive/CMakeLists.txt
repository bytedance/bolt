# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
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

bolt_add_library(bolt_hive_config OBJECT HiveConfig.cpp)

target_link_libraries(bolt_hive_config bolt_exception)

bolt_add_library(
  bolt_hive_connector
  FileHandle.cpp
  HiveConfig.cpp
  HiveConnector.cpp
  HiveConnectorSplit.cpp
  HiveConnectorUtil.cpp
  HiveDataSink.cpp
  HiveDataSource.cpp
  HivePartitionUtil.cpp
  HiveSplitReaderBase.cpp
  PaimonMetadataColumn.cpp
  PaimonSplitReader.cpp
  paimon_merge_engines/AggregateEngine.cpp
  paimon_merge_engines/DeduplicateEngine.cpp
  paimon_merge_engines/PaimonRowKind.cpp
  paimon_merge_engines/PartialUpdateEngine.cpp
  PaimonConnectorSplit.cpp
  PaimonMiscHelpers.cpp
  PaimonRowIterator.cpp
  PaimonSplitReader.cpp
  PartitionIdGenerator.cpp
  SplitReader.cpp
  TableHandle.cpp
)

set(ENABLED_DWIO_FORMATS "")
if(BOLT_ENABLE_ORC)
  list(APPEND ENABLED_DWIO_FORMATS bolt_dwio_orc_reader bolt_dwio_orc_writer)
endif()
if(BOLT_ENABLE_PARQUET)
  list(APPEND ENABLED_DWIO_FORMATS bolt_dwio_parquet_reader bolt_dwio_parquet_writer)
endif()

if(BOLT_ENABLE_TXT)
  list(APPEND ENABLED_DWIO_FORMATS bolt_dwio_txt_reader bolt_dwio_txt_writer)
endif()

set(ENABLED_FILESYSTEMS "")

list(APPEND ENABLED_FILESYSTEMS "bolt_s3fs")
list(APPEND ENABLED_FILESYSTEMS "bolt_gcs")
list(APPEND ENABLED_FILESYSTEMS "bolt_hdfs")
list(APPEND ENABLED_FILESYSTEMS "bolt_abfs")

target_link_libraries(
  bolt_hive_connector
  PUBLIC bolt_common_io
         bolt_connector
         bolt_dwio_catalog_fbhive
         bolt_dwio_dwrf_reader
         bolt_dwio_dwrf_writer
         bolt_dwio_orc_reader
         bolt_dwio_parquet_reader
         bolt_dwio_parquet_writer
         ${ENABLED_FILESYSTEMS}
         ${ENABLED_DWIO_FORMATS}
         bolt_dwio_paimon_deletion_vector_reader
         bolt_file
         bolt_hive_partition_function
         arrow::arrow
)

bolt_add_library(bolt_hive_partition_function HivePartitionFunction.cpp)

target_link_libraries(bolt_hive_partition_function bolt_core bolt_exec)

add_subdirectory(storage_adapters)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
