add_executable(
  bolt_exec_bm_test
  BmPressureAwareBlockArenaTest.cpp
  BmRowLayoutTest.cpp
  BmRowContainerBasicTest.cpp
  BmRowContainerCompareTest.cpp
  BmRowContainerExtractTest.cpp
  BmRowContainerSpillTest.cpp
  BmRowContainerStoreTest.cpp
)

add_test(
  NAME bolt_exec_bm_test
  COMMAND bolt_exec_bm_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  bolt_exec_bm_test
  bolt_exec_bm
  bolt_aggregates
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
)
