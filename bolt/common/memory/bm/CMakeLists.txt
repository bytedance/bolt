add_subdirectory(io)
add_subdirectory(file)
add_subdirectory(compress)

add_library(
  bolt_memory_bm
  AllocateSize.cpp
  BlockHandle.cpp
  BufferHandle.cpp
  BufferManager.cpp
  BufferManagerObservability.cpp
  BufferManagerReclaimer.cpp
  EvictionQueue.cpp
  MemoryTag.cpp
  OwnedFileExtent.cpp
  SpillCodec.cpp
  SpillIo.cpp
  SpillStore.cpp)

target_compile_features(bolt_memory_bm PUBLIC cxx_std_20)

target_include_directories(bolt_memory_bm PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm
  PUBLIC bolt_memory_bm_file
         bolt_memory_bm_io
         bolt_memory_bm_compress
         bolt_memory
         bolt_common_base
         bolt_common_config
         bolt_exception
         Folly::folly
         glog::glog)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmark)
endif()
