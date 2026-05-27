add_library(
  bolt_memory_bm
  AdaptiveDepthController.cpp
  DiskIoScheduler.cpp
  IoUringBackend.cpp
)

target_compile_features(bolt_memory_bm PUBLIC cxx_std_20)

target_include_directories(bolt_memory_bm PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm
  PUBLIC bolt_common_base
         bolt_exception
         Folly::folly
         glog::glog
)

if(KERNEL_SUPPORTS_IO_URING)
  find_package(liburing REQUIRED)
  target_link_libraries(bolt_memory_bm PUBLIC liburing::liburing)
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
