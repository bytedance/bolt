add_library(
  bolt_memory_bm_io
  AdaptiveDepthController.cpp
  DepthControlStats.cpp
  DepthController.cpp
  DiskIoScheduler.cpp
  DiskIoSchedulerImpl.cpp
  DiskIoSchedulerStats.cpp
  DiskIoStatsCollector.cpp
  DrrDispatcher.cpp
  EventFd.cpp
  FixedDepthController.cpp
  InflightRegistry.cpp
  IoRequestQueue.cpp
  IoUringBackend.cpp
)

target_compile_features(bolt_memory_bm_io PUBLIC cxx_std_20)

target_include_directories(bolt_memory_bm_io PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm_io
  PUBLIC bolt_memory_bm_base
         bolt_common_base
         bolt_exception
         Folly::folly
         glog::glog
)

if(KERNEL_SUPPORTS_IO_URING)
  find_package(liburing REQUIRED)
  target_link_libraries(bolt_memory_bm_io PUBLIC liburing::liburing)
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmark)
endif()
