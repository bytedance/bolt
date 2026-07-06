# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

find_package(Paimon CONFIG REQUIRED)

bolt_add_library(
    bolt_connector_paimon
    PaimonConnector.cpp
    PaimonConfig.cpp
    PaimonDataSource.cpp
    PaimonFilterTranslator.cpp
    PaimonParquetReader.cpp
    BoltMemoryPool.h
)

if(${BOLT_ENABLE_HDFS})
  target_sources(bolt_connector_paimon PRIVATE PaimonBoltHdfsFileSystem.cpp)
endif()

target_link_libraries(
  bolt_connector_paimon
  PUBLIC bolt_common_io
         bolt_connector
         $<$<BOOL:${BOLT_ENABLE_HDFS}>:bolt_hdfs>
         bolt_dwio_parquet_reader
         bolt_dwio_parquet_writer
         bolt_file
         bolt_dwio_common
         bolt_type
         bolt_vector
         arrow::arrow
         Paimon::core
        $<LINK_LIBRARY:WHOLE_ARCHIVE,Paimon::format_avro>
        $<LINK_LIBRARY:WHOLE_ARCHIVE,Paimon::fs_local>
    PRIVATE
        Paimon::libavrocpp
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmark)
endif()
