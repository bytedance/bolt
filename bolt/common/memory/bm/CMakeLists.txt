if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

add_library(bolt_memory_bm INTERFACE)

target_include_directories(bolt_memory_bm INTERFACE ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm
  INTERFACE bolt_common_base
            bolt_exception
            Folly::folly
            glog::glog
)
