include(GoogleTest)

add_executable(
  bolt_memory_bm_file_test
  FileBlockAllocatorConfigTest.cpp
  FileBlockAllocatorFactoryTest.cpp
  FileBlockAllocatorFreeTest.cpp
  FileBlockAllocatorImplTest.cpp
)

target_compile_features(bolt_memory_bm_file_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_file_test
  PRIVATE bolt_memory_bm_file
          GTest::gmock
          GTest::gtest
          GTest::gtest_main
          pthread
)

gtest_add_tests(bolt_memory_bm_file_test "" AUTO)

target_sources(
  bolt_memory_bm_file_test
  PRIVATE $<TARGET_OBJECTS:bolt_exception>
          $<TARGET_OBJECTS:bolt_flag_definitions>
          $<TARGET_OBJECTS:bolt_process>
)
