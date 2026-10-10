include(GoogleTest)

add_executable(
  bolt_memory_bm_file_test
  FileSegmentAllocatorConfigTest.cpp
  FileSegmentAllocatorFactoryTest.cpp
  FileSegmentAllocatorFreeTest.cpp
  FileSegmentAllocatorImplTest.cpp
  ManagedFileSegmentTest.cpp
  ManagedOpenFileFactoryTest.cpp
)

target_compile_features(bolt_memory_bm_file_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_file_test
  PRIVATE
    bolt_memory_bm_file
    bolt_gtest_main
    GTest::gmock
)

gtest_add_tests(
  bolt_memory_bm_file_test
  ""
  AUTO
)
