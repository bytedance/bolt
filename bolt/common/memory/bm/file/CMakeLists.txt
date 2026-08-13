add_library(
  bolt_memory_bm_file
  BucketPlacer.cpp
  DedicatedPlacer.cpp
  SegmentRegistry.cpp
  SegmentFilePath.cpp
  FileSegmentAllocator.cpp
  FileSegmentAllocatorConfig.cpp
  FileSegmentAllocatorImpl.cpp
  ManagedFileSegment.cpp
  ManagedOpenFile.cpp
  ManagedOpenFileFactory.cpp
)

target_compile_features(bolt_memory_bm_file PUBLIC cxx_std_20)

target_include_directories(bolt_memory_bm_file PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm_file
  PUBLIC bolt_memory_bm_base
         bolt_common_base
         bolt_exception
         Folly::folly
         glog::glog
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
