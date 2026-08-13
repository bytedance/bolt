add_library(
  bolt_memory_bm_compress
  CompressionAlgorithm.cpp
  CompressionManager.cpp
  CompressionRecord.cpp
  Lz4Compression.cpp
  SnappyCompression.cpp
  SpillRecordHeader.cpp
  ZstdCompression.cpp
)

target_compile_features(bolt_memory_bm_compress PUBLIC cxx_std_20)

target_include_directories(bolt_memory_bm_compress PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm_compress
  PUBLIC bolt_common_base
         bolt_common_compression
         bolt_exception
         lz4::lz4
         zstd::libzstd_static
         Snappy::snappy
         glog::glog)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
