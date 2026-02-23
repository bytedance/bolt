# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

find_package(Paimon CONFIG REQUIRED)

add_library(
    bolt_connector_paimon
    PaimonConnector.cpp
    PaimonDataSource.cpp
    PaimonParquetReader.cpp
    BoltMemoryPool.h
)

target_link_libraries(
    bolt_connector_paimon
    bolt_connector
    Paimon::core
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

