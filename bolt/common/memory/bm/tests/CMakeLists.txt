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

target_link_libraries(
  bolt_memory_bm_test
  PRIVATE
    bolt_memory_bm
    bolt_gtest_main
    GTest::gmock
)

gtest_add_tests(
  bolt_memory_bm_test
  ""
  AUTO
)
