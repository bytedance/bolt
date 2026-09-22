# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

add_executable(bolt_dwio_native_lance_test NativeLanceTest.cpp)
add_test(
  NAME bolt_dwio_native_lance_test
  COMMAND
    bolt_dwio_native_lance_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(bolt_dwio_native_lance_test PRIVATE bolt_gtest_main)

add_executable(bolt_dwio_native_lance_table_scan_test NativeLanceTableScanTest.cpp)
add_test(
  NAME bolt_dwio_native_lance_table_scan_test
  COMMAND
    bolt_dwio_native_lance_table_scan_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(bolt_dwio_native_lance_table_scan_test PRIVATE bolt_gtest_main)

if(BOLT_BUILD_BENCHMARKS AND BOLT_ENABLE_PARQUET)
  add_executable(bolt_dwio_native_lance_reader_benchmark NativeLanceReaderBenchmark.cpp)
  target_link_libraries(
    bolt_dwio_native_lance_reader_benchmark
    PRIVATE
      bolt_testutils
      bolt_dwio_native_parquet_reader
      ${FOLLY_BENCHMARK}
  )
endif()

add_executable(bolt_dwio_native_lance_list_offsets_benchmark NativeLanceListOffsetsBenchmark.cpp)
target_link_libraries(
  bolt_dwio_native_lance_list_offsets_benchmark
  PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
)

add_executable(bolt_dwio_native_lance_bitpack_benchmark NativeLanceBitpackBenchmark.cpp)
target_link_libraries(
  bolt_dwio_native_lance_bitpack_benchmark
  PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
)

add_executable(bolt_dwio_native_lance_bitmap_benchmark NativeLanceBitmapBenchmark.cpp)
target_link_libraries(
  bolt_dwio_native_lance_bitmap_benchmark
  PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
)

add_executable(bolt_dwio_native_lance_packed_struct_benchmark NativeLancePackedStructBenchmark.cpp)
target_link_libraries(
  bolt_dwio_native_lance_packed_struct_benchmark
  PRIVATE
    bolt_testutils
    ${FOLLY_BENCHMARK}
)

add_executable(
  bolt_dwio_native_lance_parallel_decode_benchmark
  NativeLanceParallelDecodeBenchmark.cpp
)
target_link_libraries(
  bolt_dwio_native_lance_parallel_decode_benchmark
  PRIVATE
    bolt_testutils
    zstd::libzstd_static
    ${FOLLY_BENCHMARK}
)
