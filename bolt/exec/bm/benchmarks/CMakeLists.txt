add_executable(
  bolt_bm_row_container_benchmark
  BmRowContainerBenchmark.cpp
  BmRowContainerBenchmarkUtil.cpp
  BmRowContainerReadMemoryBenchmark.cpp
  BmRowContainerReadSpillBenchmark.cpp
  BmRowContainerSpillBenchmark.cpp
  BmRowContainerWriteBenchmark.cpp
)

target_link_libraries(
  bolt_bm_row_container_benchmark PRIVATE
    bolt_exec_bm
    bolt_exec
    bolt_testutils
    ${FOLLY_BENCHMARK}
    GTest::gtest_main
)
