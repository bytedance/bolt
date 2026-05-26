include(GoogleTest)

add_executable(
  bolt_memory_bm_test
  ../MockIoBackend.cpp
  DiskIoSchedulerTest.cpp
)

target_compile_features(bolt_memory_bm_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_test
  PRIVATE bolt_memory_bm
          GTest::gmock
          GTest::gtest
          GTest::gtest_main
          pthread
)

gtest_add_tests(bolt_memory_bm_test "" AUTO)
