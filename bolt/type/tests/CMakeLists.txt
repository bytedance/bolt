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
  bolt_type_test
  BigDecimalTest.cpp
  ConversionsTest.cpp
  DecimalTest.cpp
  FilterSerDeTest.cpp
  FilterTest.cpp
  FloatingDecimalTest.cpp
  FloatingPointFiltersTest.cpp
  FloatingPointUtilTest.cpp
  HugeIntTest.cpp
  StringViewTest.cpp
  SubfieldTest.cpp
  TimestampConversionTest.cpp
  TimestampTest.cpp
  TypeTest.cpp
  VariantTest.cpp
)

add_test(bolt_type_test bolt_type_test)

target_link_libraries(
  bolt_type_test
  bolt_type
  bolt_common_base
  ${FOLLY_WITH_DEPENDENCIES}
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  gflags::gflags
  glog::glog
)

add_executable(bolt_filter_benchmark FilterBenchmark.cpp)

target_link_libraries(
  bolt_filter_benchmark
  bolt_type
  bolt_serialization
  Folly::folly
  ${FOLLY_BENCHMARK}
  GTest::gtest
  GTest::gtest_main
  gflags::gflags
  glog::glog
)

add_executable(bolt_negated_values_filter_benchmark NegatedValuesFilterBenchmark.cpp)

target_link_libraries(
  bolt_negated_values_filter_benchmark
  bolt_type
  bolt_serialization
  Folly::folly
  ${FOLLY_BENCHMARK}
  GTest::gtest
  GTest::gtest_main
  gflags::gflags
  glog::glog
)

add_executable(bolt_negated_bytes_values_benchmark NegatedBytesValuesBenchmark.cpp)

target_link_libraries(
  bolt_negated_bytes_values_benchmark
  bolt_type
  bolt_serialization
  Folly::folly
  ${FOLLY_BENCHMARK}
  GTest::gtest
  GTest::gtest_main
  gflags::gflags
  glog::glog
)

add_executable(bolt_negated_bigint_range_benchmark NegatedBigintRangeBenchmark.cpp)

target_link_libraries(
  bolt_negated_bigint_range_benchmark
  bolt_type
  bolt_serialization
  Folly::folly
  ${FOLLY_BENCHMARK}
  GTest::gtest
  GTest::gtest_main
  gflags::gflags
  glog::glog
)

add_executable(bolt_negated_bytes_range_benchmark NegatedBytesRangeBenchmark.cpp)

target_link_libraries(
  bolt_negated_bytes_range_benchmark
  bolt_type
  bolt_serialization
  Folly::folly
  ${FOLLY_BENCHMARK}
  GTest::gtest
  GTest::gtest_main
  gflags::gflags
  glog::glog
)

add_executable(bolt_string_view_benchmark StringViewBenchmark.cpp)

target_link_libraries(
  bolt_string_view_benchmark
  bolt_type
  Folly::folly
  ${FOLLY_BENCHMARK}
  bolt_process
  GTest::gtest
  GTest::gtest_main
  gflags::gflags
  glog::glog
)
