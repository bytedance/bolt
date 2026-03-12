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

target_link_libraries(
  bolt_connector_paimon
  PUBLIC bolt_common_io
         bolt_connector
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

