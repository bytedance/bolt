# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

find_package(Paimon CONFIG REQUIRED)

bolt_add_library(
    bolt_connector_paimon
    PaimonConnector.cpp
    PaimonDataSource.cpp
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
    PRIVATE
        $<LINK_LIBRARY:WHOLE_ARCHIVE,Paimon::format_avro>
        Paimon::libavrocpp
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmark)
endif()
