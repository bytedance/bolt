# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

add_executable(bolt_paimon_connector_test
    PaimonConnectorTest.cpp
    PaimonParquetReaderTest.cpp
)

add_test(bolt_paimon_connector_test bolt_paimon_connector_test)

target_link_libraries(bolt_paimon_connector_test
    PUBLIC
    bolt_connector_paimon
    bolt_testutils
    bolt_engine
    bolt_process
    Paimon::core
    roaring::roaring
    arrow::arrow
    Snappy::snappy
    thrift::thrift
    protobuf::protobuf
    zstd::libzstd_static
    LZ4::lz4_static
    GTest::gtest
    GTest::gtest_main
    glog::glog
    gflags::gflags
    Folly::folly
    TBB::tbb
    PRIVATE
    $<LINK_LIBRARY:WHOLE_ARCHIVE,paimon_avro_file_format>
    $<LINK_LIBRARY:WHOLE_ARCHIVE,paimon_local_file_system>
    Paimon::libavrocpp
)
