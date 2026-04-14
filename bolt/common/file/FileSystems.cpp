/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/file/FileSystems.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/synchronization/CallOnce.h>
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/file/File.h"

#include <cstdio>
#include <filesystem>
namespace bytedance::bolt::filesystems {

namespace {

constexpr std::string_view kFileScheme("file:");

using RegisteredFileSystems = std::vector<std::pair<
    std::function<bool(std::string_view)>,
    std::function<std::shared_ptr<FileSystem>(
        std::shared_ptr<const config::ConfigBase>,
        std::string_view)>>>;

RegisteredFileSystems& registeredFileSystems() {
  // Meyers singleton.
  static RegisteredFileSystems* fss = new RegisteredFileSystems();
  return *fss;
}

} // namespace

std::map<std::string, std::string> getConfFromString(
    const std::string& confstr) {
  std::map<std::string, std::string> ret;
  if (confstr.empty())
    return ret;

  size_t pos = 0;
  while (pos < confstr.length()) {
    auto end = confstr.find_first_of(";|", pos);
    if (end != std::string::npos) {
      auto mid = confstr.find(",", pos);
      if (mid != std::string::npos && mid < end) {
        ret.insert(std::make_pair(
            confstr.substr(pos, mid - pos),
            confstr.substr(mid + 1, end - mid - 1)));
      }
      pos = end + 1;
    } else {
      auto mid = confstr.find(",", pos);
      if (mid != std::string::npos) {
        ret.insert(std::make_pair(
            confstr.substr(pos, mid - pos), confstr.substr(mid + 1)));
      }
      pos = confstr.length();
    }
  }
  return ret;
}

void registerFileSystem(
    std::function<bool(std::string_view)> schemeMatcher,
    std::function<std::shared_ptr<FileSystem>(
        std::shared_ptr<const config::ConfigBase>,
        std::string_view)> fileSystemGenerator) {
  registeredFileSystems().emplace_back(schemeMatcher, fileSystemGenerator);
}

std::shared_ptr<FileSystem> getFileSystem(
    std::string_view filePath,
    std::shared_ptr<const config::ConfigBase> properties) {
  const auto& filesystems = registeredFileSystems();
  for (const auto& p : filesystems) {
    if (p.first(filePath)) {
      return p.second(properties, filePath);
    }
  }
  BOLT_FAIL("No registered file system matched with file path '{}'", filePath);
}

namespace {

folly::once_flag localFSInstantiationFlag;

// Implement Local FileSystem.
class LocalFileSystem : public FileSystem {
 public:
  explicit LocalFileSystem(
      std::shared_ptr<const config::ConfigBase> config,
      const FileSystemOptions& options)
      : FileSystem(config),
        executor_(
            options.readAheadEnabled
                ? std::make_unique<folly::CPUThreadPoolExecutor>(
                      std::max(
                          1,
                          static_cast<int32_t>(
                              std::thread::hardware_concurrency() / 2)),
                      std::make_shared<folly::NamedThreadFactory>(
                          "LocalReadahead"))
                : nullptr) {}

  ~LocalFileSystem() override {
    if (executor_) {
      executor_->stop();
      LOG(INFO) << "Executor " << executor_->getName() << " stopped.";
    }
  }

  std::string name() const override {
    return "Local FS";
  }

  inline std::string_view extractPath(std::string_view path) const override {
    if (path.find(kFileScheme) == 0) {
      return path.substr(kFileScheme.length());
    }
    return path;
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& /*unused*/) override {
    return std::make_unique<LocalReadFile>(extractPath(path), executor_.get());
  }

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options) override {
    return std::make_unique<LocalWriteFile>(
        extractPath(path),
        options.shouldCreateParentDirectories,
        options.shouldThrowOnFileAlreadyExists);
  }
#ifdef IO_URING_SUPPORTED
  std::unique_ptr<ReadFile> openAsyncFileForRead(
      std::string_view path,
      const FileOptions& /*unused*/) override {
    return std::make_unique<AsyncLocalReadFile>(extractPath(path));
  }

  std::unique_ptr<WriteFile> openAsyncFileForWrite(
      std::string_view path,
      const FileOptions& /*unused*/) override {
    return std::make_unique<AsyncLocalWriteFile>(
        extractPath(path), false, true);
  }
#endif

  void remove(std::string_view path) override {
    auto file = extractPath(path);
    int32_t rc = std::remove(std::string(file).c_str());
    if (rc < 0 && std::filesystem::exists(file)) {
      BOLT_USER_FAIL(
          "Failed to delete file {} with errno {}", file, strerror(errno));
    }
    VLOG(1) << "LocalFileSystem::remove " << path;
  }

  void rename(
      std::string_view oldPath,
      std::string_view newPath,
      bool overwrite) override {
    auto oldFile = extractPath(oldPath);
    auto newFile = extractPath(newPath);
    if (!overwrite && exists(newPath)) {
      BOLT_USER_FAIL(
          "Failed to rename file {} to {} with as {} exists.",
          oldFile,
          newFile,
          newFile);
      return;
    }
    int32_t rc =
        ::rename(std::string(oldFile).c_str(), std::string(newFile).c_str());
    if (rc != 0) {
      BOLT_USER_FAIL(
          "Failed to rename file {} to {} with errno {}",
          oldFile,
          newFile,
          folly::errnoStr(errno));
    }
    VLOG(1) << "LocalFileSystem::rename oldFile: " << oldFile
            << ", newFile:" << newFile;
  }

  bool exists(std::string_view path) override {
    const auto file = extractPath(path);
    return std::filesystem::exists(file);
  }

  bool isDirectory(std::string_view path) const override {
    const auto file = extractPath(path);
    return std::filesystem::is_directory(file);
  }

  virtual std::vector<std::string> list(std::string_view path) override {
    auto directoryPath = extractPath(path);
    const std::filesystem::path folder{directoryPath};
    std::vector<std::string> filePaths;
    for (auto const& entry : std::filesystem::directory_iterator{folder}) {
      filePaths.push_back(entry.path());
    }
    return filePaths;
  }

  void mkdir(std::string_view path) override {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    BOLT_CHECK_EQ(
        0,
        ec.value(),
        "Mkdir {} failed: {}, message: {}",
        std::string(path),
        ec.value(),
        ec.message());
    VLOG(1) << "LocalFileSystem::mkdir " << path;
  }

  void rmdir(std::string_view path) override {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    BOLT_CHECK_EQ(
        0,
        ec.value(),
        "Rmdir {} failed: {}, message: {}",
        std::string(path),
        ec.value(),
        ec.message());
    VLOG(1) << "LocalFileSystem::rmdir " << path;
  }

  static std::function<bool(std::string_view)> schemeMatcher() {
    // Note: presto behavior is to prefix local paths with 'file:'.
    // Check for that prefix and prune to absolute regular paths as needed.
    return [](std::string_view filePath) {
      return filePath.find("/") == 0 || filePath.find(kFileScheme) == 0;
    };
  }

  static std::function<std::shared_ptr<
      FileSystem>(std::shared_ptr<const config::ConfigBase>, std::string_view)>
  fileSystemGenerator(const FileSystemOptions& options) {
    return [&options](
               std::shared_ptr<const config::ConfigBase> properties,
               std::string_view filePath) {
      // One instance of Local FileSystem is sufficient.
      // Initialize on first access and reuse after that.
      static std::shared_ptr<FileSystem> lfs;
      folly::call_once(localFSInstantiationFlag, [&properties, &options]() {
        lfs = std::make_shared<LocalFileSystem>(properties, options);
      });
      return lfs;
    };
  }

 private:
  const std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};
} // namespace

void registerLocalFileSystem(const FileSystemOptions& options) {
  registerFileSystem(
      LocalFileSystem::schemeMatcher(),
      LocalFileSystem::fileSystemGenerator(options));
}
} // namespace bytedance::bolt::filesystems
