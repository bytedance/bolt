# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

add_executable(
    bolt_paimon_connector_benchmark
    PaimonBenchmark.cpp
)

find_package(TBB REQUIRED)

target_link_libraries(
    bolt_paimon_connector_benchmark
    PRIVATE
    bolt_connector_paimon
    bolt_hive_connector
    bolt_exec_test_lib
    bolt_query_benchmark
    bolt_benchmark_builder
    TBB::tbb
    Paimon::core
    $<LINK_LIBRARY:WHOLE_ARCHIVE,paimon_local_file_system>
    $<LINK_LIBRARY:WHOLE_ARCHIVE,paimon_avro_file_format>
    Paimon::libavrocpp
)
