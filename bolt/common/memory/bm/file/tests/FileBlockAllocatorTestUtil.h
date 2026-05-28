#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

#include <filesystem>
#include <string>

namespace bytedance::bolt::memory::bm::test {

inline FileBlockAllocatorConfig ValidConfig() {
  FileBlockAllocatorConfig config;
  config.directory = "/tmp/bolt-bm-file-allocator-test";
  config.bucket_sizes = {4 * 1024, 8 * 1024, 16 * 1024};
  config.file_size_limit_bytes = 64 * 1024;
  config.max_open_files_per_bucket = 2;
  return config;
}

inline std::string UniqueTempDir(const std::string& name) {
  return (std::filesystem::temp_directory_path() / name).string();
}

inline FileBlockAllocatorConfig ValidConfigWithDirectory(
    const std::string& path) {
  auto config = ValidConfig();
  config.directory = path;
  return config;
}

} // namespace bytedance::bolt::memory::bm::test
