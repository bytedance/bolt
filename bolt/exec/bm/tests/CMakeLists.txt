include(GoogleTest)

add_executable(
  bolt_exec_bm_test
  ../benchmarks/BmRowContainerBenchmarkCommon.cpp
  ../benchmarks/BmRowContainerBenchmarkProfiles.cpp
  BmRowContainerBenchmarkProfileTest.cpp
  BmRowContainerComplexBenchmarkTest.cpp
  BmMergeReadSessionTest.cpp
  BmPartitionTest.cpp
  BmRowContainerBatchTest.cpp
  BmRowContainerReadTest.cpp
  BmRowContainerResidentTest.cpp
  BmStreamingWindowBuildTest.cpp
  BmTaskConfigTest.cpp
  BmWindowPartitionTest.cpp
  BmWindowSemanticCoverageTest.cpp
  BmSegmentCollectionTest.cpp
)

target_compile_features(bolt_exec_bm_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_exec_bm_test
  PRIVATE
    bolt_exec_bm
    bolt_gtest_main
    GTest::gmock
)

gtest_add_tests(
  bolt_exec_bm_test
  ""
  AUTO
)

# Isolate the singleton execution-memory configuration used by admission tests.
add_executable(bolt_bm_bulkread_admission_test BmBulkReadAdmissionTest.cpp)
target_link_libraries(
  bolt_bm_bulkread_admission_test
  PRIVATE
    bolt_exec_bm
    bolt_gtest_main
    GTest::gmock
)
gtest_add_tests(
  bolt_bm_bulkread_admission_test
  ""
  AUTO
)
