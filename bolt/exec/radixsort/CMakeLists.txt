# Copyright (c) ByteDance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

target_sources(
  bolt_exec PRIVATE
  RadixSortBuffer.cpp
  RadixSortRunSorter.cpp
  RadixSortRunStorage.cpp
  RadixSortKeyCodec.cpp
  RadixSortKey.cpp
  RadixSortSpill.cpp
  RadixSortSpillRow.cpp
  PayloadRowLayout.cpp
  PayloadRowMeasure.cpp
  PayloadRowReader.cpp
  PayloadRowWriter.cpp
  RadixSortRun.cpp
)

if(BOLT_BUILD_TESTING OR BOLT_BUILD_RADIX_SORT_TEST)
  add_executable(
    bolt_radix_sort_test
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/utils/TempDirectoryPath.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/utils/RadixSortComparatorOracle.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/RadixSortKeyCodecTest.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/RadixSortKeyTest.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/RadixPayloadRowTest.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/RadixSortSpillRowTest.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/RadixSortRunSorterTest.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/RadixSortBufferTest.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/RadixSortRunTest.cpp
  )

  target_link_libraries(
    bolt_radix_sort_test
    bolt_gtest_main
  )

  add_test(
    NAME bolt_radix_sort_test
    COMMAND bolt_radix_sort_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )
endif()

if(BOLT_BUILD_BENCHMARKS OR BOLT_BUILD_RADIX_SORT_BENCHMARK)
  add_executable(
    bolt_radix_sort_buffer_benchmark
    ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks/RadixSortBenchmarkData.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks/RadixSortBufferBenchmark.cpp
  )

  target_link_libraries(
    bolt_radix_sort_buffer_benchmark PRIVATE
    bolt_engine
    Folly::follybenchmark
  )

  add_executable(
    bolt_radix_sort_buffer_large_benchmark
    ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks/RadixSortBenchmarkData.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks/RadixSortBufferBenchmark.cpp
  )

  target_compile_definitions(
    bolt_radix_sort_buffer_large_benchmark
    PRIVATE RADIX_SORT_LARGE_BENCHMARK=1
  )

  target_link_libraries(
    bolt_radix_sort_buffer_large_benchmark PRIVATE
    bolt_engine
    Folly::follybenchmark
  )

  add_executable(
    bolt_radix_sort_spill_benchmark
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/utils/TempDirectoryPath.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks/RadixSortBenchmarkData.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks/RadixSortSpillBenchmark.cpp
  )

  target_link_libraries(
    bolt_radix_sort_spill_benchmark PRIVATE
    bolt_engine
    Folly::follybenchmark
  )

  add_executable(
    bolt_radix_sort_spill_large_benchmark
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/utils/TempDirectoryPath.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks/RadixSortBenchmarkData.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks/RadixSortSpillBenchmark.cpp
  )

  target_compile_definitions(
    bolt_radix_sort_spill_large_benchmark
    PRIVATE RADIX_SORT_LARGE_BENCHMARK=1
  )

  target_link_libraries(
    bolt_radix_sort_spill_large_benchmark PRIVATE
    bolt_engine
    Folly::follybenchmark
  )

  add_executable(
    bolt_radix_sort_algorithm_benchmark
    ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks/RadixSortAlgorithmBenchmark.cpp
  )

  target_link_libraries(
    bolt_radix_sort_algorithm_benchmark PRIVATE
    bolt_engine
    Folly::follybenchmark
  )
endif()
