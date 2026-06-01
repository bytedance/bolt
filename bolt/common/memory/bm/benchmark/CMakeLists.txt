add_executable(
  bolt_memory_bm_sort_benchmark
  BufferManagerSortBenchmark.cpp
  SparkListenableArbitratorContext.cpp)

add_executable(
  bolt_memory_bm_parallel_sort_benchmark
  BufferManagerParallelSortBenchmark.cpp
  SparkListenableArbitratorContext.cpp)

add_executable(
  bolt_memory_bm_parallel_sort_prefetch_benchmark
  BufferManagerParallelSortPrefetchBenchmark.cpp
  SparkListenableArbitratorContext.cpp)

target_compile_features(bolt_memory_bm_sort_benchmark PRIVATE cxx_std_20)
target_compile_features(
  bolt_memory_bm_parallel_sort_benchmark PRIVATE cxx_std_20)
target_compile_features(
  bolt_memory_bm_parallel_sort_prefetch_benchmark PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_sort_benchmark
  PRIVATE bolt_memory_bm
          bolt_flag_definitions
          bolt_process
          gflags::gflags
          glog::glog
          pthread)

target_link_libraries(
  bolt_memory_bm_parallel_sort_benchmark
  PRIVATE bolt_memory_bm
          bolt_flag_definitions
          bolt_process
          gflags::gflags
          glog::glog
          pthread)

target_link_libraries(
  bolt_memory_bm_parallel_sort_prefetch_benchmark
  PRIVATE bolt_memory_bm
          bolt_flag_definitions
          bolt_process
          gflags::gflags
          glog::glog
          pthread)
