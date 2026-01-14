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

add_executable(bolt_shuffle_spark_celeborn_test CelebornClientTest.cpp)

add_test(bolt_shuffle_spark_celeborn_test bolt_shuffle_spark_celeborn_test)

target_link_libraries(
    bolt_shuffle_spark_celeborn_test
    PRIVATE
        bolt_shuffle_spark_impl
        bolt_exception
        gflags::gflags
        glog::glog
        GTest::gtest
        GTest::gtest_main
)

add_executable(
    bolt_shuffle_spark_matrix_tests
    ShuffleTestBase.cpp
    ShuffleMatrixTest.cpp
)

target_link_libraries(
    bolt_shuffle_spark_matrix_tests
    PRIVATE
        bolt_shuffle_spark_impl
        bolt_exec_test_lib
        bolt_vector_test_lib
        bolt_vector_fuzzer
        bolt_temp_path
        GTest::gtest_main
        GTest::gmock
)

# Add test
add_test(
    NAME bolt_shuffle_spark_matrix_tests
    COMMAND bolt_shuffle_spark_matrix_tests
)

add_executable(
    bolt_shuffle_spark_large_partition_tests
    ShuffleTestBase.cpp
    ShuffleLargePartitionTest.cpp
)

target_link_libraries(
    bolt_shuffle_spark_large_partition_tests
    PRIVATE
        bolt_shuffle_spark_impl
        bolt_exec_test_lib
        bolt_vector_test_lib
        bolt_vector_fuzzer
        bolt_temp_path
        GTest::gtest_main
        GTest::gmock
)

# Add test
add_test(
    NAME bolt_shuffle_spark_large_partition_tests
    COMMAND bolt_shuffle_spark_large_partition_tests
)

add_executable(
    bolt_shuffle_spark_memory_tests
    ShuffleTestBase.cpp
    ShuffleMemoryTest.cpp
)

target_link_libraries(
    bolt_shuffle_spark_memory_tests
    PRIVATE
        bolt_shuffle_spark_impl
        bolt_exec_test_lib
        bolt_vector_test_lib
        bolt_vector_fuzzer
        GTest::gtest_main
        GTest::gmock
)

# Add test
add_test(
    NAME bolt_shuffle_spark_memory_tests
    COMMAND bolt_shuffle_spark_memory_tests
)
