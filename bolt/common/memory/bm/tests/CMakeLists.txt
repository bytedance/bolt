include(GoogleTest)

add_executable(
  bolt_memory_bm_test
  BlockStateMachineTest.cpp
  BlockDescriptorTest.cpp
  BufferManagerValueMappingTest.cpp
  BufferManagerExecutionMemoryPoolTest.cpp
  BufferManagerTest.cpp
  BufferManagerInternalsTest.cpp
  BufferManagerPrivateStateTest.cpp
  EvictionQueueTest.cpp
  ScopedFdTest.cpp
  ReclaimWriteWindowTest.cpp
  SparkListenableArbitratorContext.cpp
)

target_compile_features(bolt_memory_bm_test PRIVATE cxx_std_20)

add_library(bolt_memory_bm_test_compile_deps INTERFACE)
target_link_libraries(bolt_memory_bm_test_compile_deps INTERFACE bolt_memory_bm)
set(BOLT_MEMORY_BM_TEST_LIBRARY bolt_memory_bm_test_compile_deps)
if(BOLT_TEST_LINKAGE STREQUAL "static")
  set(BOLT_MEMORY_BM_TEST_LIBRARY bolt_memory_bm)
endif()

target_link_libraries(
  bolt_memory_bm_test
  PRIVATE
    ${BOLT_MEMORY_BM_TEST_LIBRARY}
    bolt_gtest_main
    GTest::gmock
)

gtest_add_tests(
  bolt_memory_bm_test
  ""
  AUTO
)
