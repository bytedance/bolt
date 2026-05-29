add_subdirectory(io)
add_subdirectory(file)

add_library(
  bolt_memory_bm
  BlockHandle.cpp
  BufferHandle.cpp
  BufferManager.cpp
  BufferManagerIo.cpp
  BufferManagerReclaimer.cpp
  EvictionQueue.cpp
  MemoryTag.cpp
  OwnedFileExtent.cpp
)

target_compile_features(bolt_memory_bm PUBLIC cxx_std_20)

target_include_directories(bolt_memory_bm PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm
  PUBLIC bolt_memory_bm_file
         bolt_memory_bm_io
         bolt_memory
         bolt_common_base
         bolt_common_config
         bolt_exception
         Folly::folly
         glog::glog
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
