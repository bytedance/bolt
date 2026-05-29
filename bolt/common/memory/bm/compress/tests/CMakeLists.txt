include(GoogleTest)

add_executable(
  bolt_memory_bm_compress_test
  CompressionCodecTest.cpp
)

target_compile_features(bolt_memory_bm_compress_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_compress_test
  PRIVATE bolt_memory_bm_compress
          GTest::gmock
          GTest::gtest
          GTest::gtest_main
          pthread
)

gtest_add_tests(bolt_memory_bm_compress_test "" AUTO)

target_sources(
  bolt_memory_bm_compress_test
  PRIVATE $<TARGET_OBJECTS:bolt_exception>
          $<TARGET_OBJECTS:bolt_flag_definitions>
          $<TARGET_OBJECTS:bolt_process>
)
