function(add_bm_row_container_benchmark TARGET)
  add_executable(
    ${TARGET}
    BmRowContainerBenchmarkCommon.cpp
    BmRowContainerBenchmarkMain.cpp
    BmRowContainerBenchmarkProfiles.cpp
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

add_executable(
  bolt_streaming_window_build_benchmark
  StreamingWindowBuildBenchmark.cpp
)

target_compile_features(
  bolt_streaming_window_build_benchmark
  PRIVATE cxx_std_20
)

target_link_libraries(
  bolt_streaming_window_build_benchmark
  PRIVATE bolt_testutils
          ${FOLLY_BENCHMARK}
          GTest::gtest_main
)
