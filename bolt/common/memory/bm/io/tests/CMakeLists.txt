include(GoogleTest)

add_executable(
  bolt_memory_bm_io_test
  MockIoBackend.cpp
  AdaptiveDepthControllerTest.cpp
  DepthControllerTest.cpp
  DiskIoSchedulerConfigTest.cpp
  DiskIoSchedulerImplTest.cpp
  DiskIoStatsCollectorTest.cpp
  DiskIoSchedulerTest.cpp
  DrrDispatcherTest.cpp
  EventFdTest.cpp
  InflightRegistryTest.cpp
  IoBufferTest.cpp
  IoPriorityTest.cpp
  IoRequestValidatorTest.cpp
  IoRequestQueueTest.cpp
  IoUringBackendTest.cpp
  MockIoBackendTest.cpp
)

target_compile_features(bolt_memory_bm_io_test PRIVATE cxx_std_20)

add_library(bolt_memory_bm_io_test_compile_deps INTERFACE)
target_link_libraries(bolt_memory_bm_io_test_compile_deps INTERFACE bolt_memory_bm_io)
set(BOLT_MEMORY_BM_IO_TEST_LIBRARY bolt_memory_bm_io_test_compile_deps)
if(BOLT_TEST_LINKAGE STREQUAL "static")
  set(BOLT_MEMORY_BM_IO_TEST_LIBRARY bolt_memory_bm_io)
endif()

target_link_libraries(
  bolt_memory_bm_io_test
  PRIVATE
    ${BOLT_MEMORY_BM_IO_TEST_LIBRARY}
    bolt_gtest_main
    GTest::gmock
)

gtest_add_tests(
  bolt_memory_bm_io_test
  ""
  AUTO
)
