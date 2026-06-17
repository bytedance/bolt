include(GoogleTest)

add_executable(
  bolt_exec_bm_test
  ../benchmarks/BmRowContainerBenchmarkProfiles.cpp
  BmRowContainerBenchmarkProfileTest.cpp
  BmMergeReadSessionTest.cpp
  BmPartitionTest.cpp
  BmRowContainerBatchTest.cpp
  BmRowContainerReadTest.cpp
  BmRowContainerResidentTest.cpp
  BmSegmentCollectionTest.cpp
  BmTestMain.cpp
)

target_compile_features(bolt_exec_bm_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_exec_bm_test
  PRIVATE bolt_exec_bm
          bolt_testutils
          GTest::gmock
          GTest::gtest
          pthread
)

gtest_add_tests(bolt_exec_bm_test "" AUTO)
