bolt_add_library(
  bolt_exec_bm
  BmPressureAwareBlockArena.cpp
  BmRowContainer.cpp
)

target_compile_features(bolt_exec_bm PRIVATE cxx_std_20)

target_link_libraries(
  bolt_exec_bm
  bolt_memory_bm
  bolt_vector
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
