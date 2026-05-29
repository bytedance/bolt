#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

#include <filesystem>
#include <string>
#include <vector>

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

inline std::vector<std::filesystem::path> ListDirectories(
    const std::filesystem::path& directory) {
  std::vector<std::filesystem::path> directories;
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (entry.is_directory()) {
      directories.push_back(entry.path());
    }
  }
  return directories;
}

inline std::filesystem::path OnlyAllocatorDirectory(
    const std::filesystem::path& directory) {
  const auto directories = ListDirectories(directory);
  if (directories.size() != 1) {
    return {};
  }
  return directories[0];
}

} // namespace bytedance::bolt::memory::bm::test
