add_executable(
  bolt_exec_bm_row_container_benchmark
  BmRowContainerBenchmarkCommon.cpp
  BmRowContainerBenchmarkMain.cpp
  BmRowContainerPipelineBenchmark.cpp
  BmRowContainerReadBenchmark.cpp
  BmRowContainerSpillReadBenchmark.cpp
  BmRowContainerSpillWriteBenchmark.cpp
  BmRowContainerStoreBenchmark.cpp
)

target_compile_features(bolt_exec_bm_row_container_benchmark PRIVATE cxx_std_20)

target_link_libraries(
  bolt_exec_bm_row_container_benchmark
  PRIVATE bolt_exec_bm
          bolt_testutils
          ${FOLLY_BENCHMARK}
          GTest::gtest_main
          pthread
)
