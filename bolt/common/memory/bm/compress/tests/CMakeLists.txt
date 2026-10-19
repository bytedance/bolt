include(GoogleTest)

add_executable(bolt_memory_bm_compress_test CompressionManagerTest.cpp)

target_compile_features(bolt_memory_bm_compress_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_compress_test
  PRIVATE
    bolt_memory_bm_compress
    bolt_gtest_main
    GTest::gmock
)

gtest_add_tests(
  bolt_memory_bm_compress_test
  ""
  AUTO
)
