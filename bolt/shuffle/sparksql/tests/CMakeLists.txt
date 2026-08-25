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

bolt_add_library(bolt_shuffle_spark_test_base ShuffleTestBase.cpp)
target_link_libraries(
  bolt_shuffle_spark_test_base
  PRIVATE
    bolt_engine_for_tests
    celeborn-cpp-client::celeborn-cpp-client
    duckdb::duckdb
    GTest::gtest
    GTest::gmock
)

add_executable(bolt_shuffle_spark_celeborn_test CelebornClientTest.cpp)

add_test(bolt_shuffle_spark_celeborn_test bolt_shuffle_spark_celeborn_test)

target_link_libraries(
  bolt_shuffle_spark_celeborn_test
  PRIVATE
    glog::glog
    bolt_gtest_main
)
if(BOLT_TEST_LINKAGE STREQUAL "shared")
  # The shared engine owns Celeborn's implementation; this source only needs
  # the third-party headers for its direct API tests.
  target_include_directories(
    bolt_shuffle_spark_celeborn_test
    SYSTEM
    PRIVATE
      "$<TARGET_PROPERTY:celeborn-cpp-client::celeborn-cpp-client,INTERFACE_INCLUDE_DIRECTORIES>"
  )
endif()

add_test(
  NAME bolt_shuffle_spark_celeborn_e2e_test
  COMMAND
    bolt_shuffle_spark_celeborn_test --gtest_filter=CelebornE2ESmokeTest.nativeClientPushAndReadBack
)
set_tests_properties(
  bolt_shuffle_spark_celeborn_e2e_test
  PROPERTIES
    LABELS
      "celeborn_e2e"
)

add_executable(bolt_shuffle_spark_matrix_test ShuffleMatrixTest.cpp)

target_link_libraries(
  bolt_shuffle_spark_matrix_test
  PRIVATE
    bolt_gtest_main
    GTest::gmock
)

# Add test
add_test(NAME bolt_shuffle_spark_matrix_test COMMAND bolt_shuffle_spark_matrix_test)

add_executable(bolt_shuffle_spark_large_partition_test ShuffleLargePartitionTest.cpp)

target_link_libraries(
  bolt_shuffle_spark_large_partition_test
  PRIVATE
    bolt_gtest_main
    GTest::gmock
)

# Add test
add_test(
  NAME bolt_shuffle_spark_large_partition_test
  COMMAND
    bolt_shuffle_spark_large_partition_test
)

add_executable(bolt_shuffle_spark_memory_test ShuffleMemoryTest.cpp)

target_link_libraries(
  bolt_shuffle_spark_memory_test
  PRIVATE
    bolt_gtest_main
    GTest::gmock
)

# Add test
add_test(NAME bolt_shuffle_spark_memory_test COMMAND bolt_shuffle_spark_memory_test)

add_executable(bolt_shuffle_spark_misc_test ShuffleMiscTest.cpp)

target_link_libraries(
  bolt_shuffle_spark_misc_test
  PRIVATE
    bolt_gtest_main
    GTest::gmock
)

# Add test
add_test(NAME bolt_shuffle_spark_misc_test COMMAND bolt_shuffle_spark_misc_test)

add_executable(bolt_shuffle_codec_test CodecTest.cpp)

target_link_libraries(
  bolt_shuffle_codec_test
  PRIVATE
    bolt_gtest_main
    GTest::gmock
)

add_test(NAME bolt_shuffle_codec_test COMMAND bolt_shuffle_codec_test)

add_executable(bolt_shuffle_adaptive_zstd_codec_test AdaptiveParallelZstdCodecTest.cpp)

target_link_libraries(
  bolt_shuffle_adaptive_zstd_codec_test
  PRIVATE
    bolt_gtest_main
    GTest::gmock
)

add_test(NAME bolt_shuffle_adaptive_zstd_codec_test COMMAND bolt_shuffle_adaptive_zstd_codec_test)

add_executable(bolt_shuffle_reader_test BoltShuffleReaderTest.cpp)

target_link_libraries(
  bolt_shuffle_reader_test
  PRIVATE
    bolt_gtest_main
    GTest::gmock
)

add_test(NAME bolt_shuffle_reader_test COMMAND bolt_shuffle_reader_test)

add_executable(bolt_shuffle_payload_test PayloadTest.cpp)

target_link_libraries(bolt_shuffle_payload_test PRIVATE bolt_gtest_main)

add_test(NAME bolt_shuffle_payload_test COMMAND bolt_shuffle_payload_test)

add_executable(bolt_shuffle_local_partition_writer_test LocalPartitionWriterTest.cpp)

target_link_libraries(
  bolt_shuffle_local_partition_writer_test
  PRIVATE
    bolt_gtest_main
    GTest::gmock
)

add_test(
  NAME bolt_shuffle_local_partition_writer_test
  COMMAND
    bolt_shuffle_local_partition_writer_test
)
