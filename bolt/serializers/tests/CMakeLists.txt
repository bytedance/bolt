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

add_executable(
    bolt_serializer_test
    PrestoOutputStreamListenerTest.cpp
    PrestoSerializerTest.cpp
    UnsafeRowSerializerTest.cpp
    CompactRowSerializerTest.cpp
    ArrowSerializerTest.cpp
)

add_test(bolt_serializer_test bolt_serializer_test)

target_link_libraries(
    bolt_serializer_test
    bolt_presto_serializer
    bolt_vector_test_lib
    bolt_vector_fuzzer
    bolt_row_fast
    GTest::gtest
    GTest::gtest_main
    gflags::gflags
    glog::glog
)

add_executable(bolt_serializer_benchmark SerializerBenchmark.cpp)

target_link_libraries(
    bolt_serializer_benchmark
    bolt_presto_serializer
    bolt_vector_test_lib
    bolt_vector_fuzzer
    GTest::gtest
    GTest::gtest_main
    gflags::gflags
    glog::glog
)

add_executable(bolt_arrowserde_test ArrowSerializerTest.cpp)

add_test(bolt_arrowserde_test bolt_arrowserde_test)

target_link_libraries(
    bolt_arrowserde_test
    bolt_presto_serializer
    bolt_vector_test_lib
    bolt_vector_fuzzer
    bolt_row_fast
    GTest::gtest
    GTest::gtest_main
    gflags::gflags
    glog::glog
)

add_executable(bolt_arrowserde_benchmark ArrowSerializerBenchmark.cpp)

target_link_libraries(
    bolt_arrowserde_benchmark
    bolt_presto_serializer
    bolt_vector_test_lib
    bolt_vector_fuzzer
    GTest::gtest
    GTest::gtest_main
    gflags::gflags
    glog::glog
)
