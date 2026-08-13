add_executable(
  bolt_memory_bm_io_scheduler_benchmark
  SchedulerBenchmark.cpp)

target_compile_features(bolt_memory_bm_io_scheduler_benchmark PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_io_scheduler_benchmark
  PRIVATE bolt_memory_bm_io
          bolt_common_base
          bolt_common_compression
          bolt_dwio_common_exception
          bolt_serialization
          bolt_exception
          bolt_process
          gflags::gflags
          Folly::folly
          glog::glog)

target_sources(
  bolt_memory_bm_io_scheduler_benchmark
  PRIVATE $<TARGET_OBJECTS:bolt_exception>
          $<TARGET_OBJECTS:bolt_flag_definitions>
          $<TARGET_OBJECTS:bolt_process>
)
