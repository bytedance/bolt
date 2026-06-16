function(add_bm_row_container_benchmark TARGET)
  add_executable(
    ${TARGET}
    BmRowContainerBenchmarkCommon.cpp
    BmRowContainerBenchmarkMain.cpp
    ${ARGN}
  )

  target_compile_features(${TARGET} PRIVATE cxx_std_20)

  target_link_libraries(
    ${TARGET}
    PRIVATE bolt_exec_bm
            bolt_testutils
            ${FOLLY_BENCHMARK}
            GTest::gtest_main
            pthread
  )
endfunction()

add_bm_row_container_benchmark(
  bolt_exec_bm_row_container_benchmark
  BmRowContainerReadBenchmark.cpp
  BmRowContainerBatchStoreBenchmark.cpp
  BmRowContainerSpillReadBenchmark.cpp
  BmRowContainerSpillWriteBenchmark.cpp
  BmRowContainerStoreBenchmark.cpp
)

add_bm_row_container_benchmark(
  bolt_exec_bm_row_container_pipeline_benchmark
  BmRowContainerPipelineBenchmark.cpp
)

add_bm_row_container_benchmark(
  bolt_exec_bm_row_container_pipeline_thread_benchmark
  BmRowContainerPipelineThreadBenchmark.cpp
)
