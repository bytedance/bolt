/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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
 */

#include "bolt/connectors/paimon/PaimonBoltFileSystem.h"

#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bolt/common/config/Config.h"
#include "bolt/common/file/File.h"
#include "bolt/common/file/FileSystems.h"

#include "paimon/factories/factory_creator.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace bytedance::bolt::connector::paimon {

namespace {

constexpr std::string_view kHdfsScheme{"hdfs://"};
constexpr std::string_view kIdentifier{"bolt"};

std::string uriAuthorityPrefix(const std::string& uri) {
  if (uri.rfind(std::string(kHdfsScheme), 0) != 0) {
    return {};
  }
  const auto firstSlash = uri.find('/', kHdfsScheme.size());
  if (firstSlash == std::string::npos) {
    return uri;
  }
  return uri.substr(0, firstSlash);
}

std::string joinDirAndBasename(
    const std::string& dir,
    const std::string& entry) {
  auto lastSlash = entry.rfind('/');
  std::string basename =
      (lastSlash == std::string::npos) ? entry : entry.substr(lastSlash + 1);
  if (dir.empty()) {
    return basename;
  }
  if (dir.back() == '/') {
    return dir + basename;
  }
  return dir + "/" + basename;
}

class PaimonBoltInputStream final : public ::paimon::InputStream {
 public:
  PaimonBoltInputStream(
      std::shared_ptr<bytedance::bolt::ReadFile> file,
      std::string uri)
      : file_(std::move(file)), uri_(std::move(uri)) {}

  ::paimon::Status Seek(int64_t offset, ::paimon::SeekOrigin origin) override {
    if (origin != ::paimon::FS_SEEK_SET && origin != ::paimon::FS_SEEK_CUR &&
        origin != ::paimon::FS_SEEK_END) {
      return ::paimon::Status::Invalid(
          "invalid SeekOrigin, only support FS_SEEK_SET, FS_SEEK_CUR, and FS_SEEK_END");
    }

    const int64_t size = static_cast<int64_t>(file_->size());
    int64_t base = 0;
    if (origin == ::paimon::FS_SEEK_SET) {
      base = 0;
    } else if (origin == ::paimon::FS_SEEK_CUR) {
      base = pos_.load();
    } else {
      base = size;
    }

    const int64_t next = base + offset;
    if (next < 0 || next > size) {
      return ::paimon::Status::Invalid("Seek out of range");
    }
    pos_.store(next);
    return ::paimon::Status::OK();
  }

  ::paimon::Result<int64_t> GetPos() const override {
    return pos_.load();
  }

  ::paimon::Result<int32_t> Read(char* buffer, uint32_t size) override {
    const auto offset = static_cast<uint64_t>(pos_.load());
    auto res = Read(buffer, size, offset);
    if (res.ok()) {
      pos_.fetch_add(res.value());
    }
    return res;
  }

  ::paimon::Result<int32_t> Read(char* buffer, uint32_t size, uint64_t offset)
      override {
    try {
      auto view = file_->pread(offset, size, buffer);
      return static_cast<int32_t>(view.size());
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("pread failed: ") + e.what());
    }
  }

  void ReadAsync(
      char* buffer,
      uint32_t size,
      uint64_t offset,
      std::function<void(::paimon::Status)>&& callback) override {
    auto res = Read(buffer, size, offset);
    if (res.ok()) {
      callback(::paimon::Status::OK());
    } else {
      callback(res.status());
    }
  }

  ::paimon::Result<std::string> GetUri() const override {
    return uri_;
  }

  ::paimon::Result<uint64_t> Length() const override {
    return file_->size();
  }

  ::paimon::Status Close() override {
    file_.reset();
    return ::paimon::Status::OK();
  }

 private:
  std::shared_ptr<bytedance::bolt::ReadFile> file_;
  std::string uri_;
  std::atomic<int64_t> pos_{0};
};

class PaimonBoltOutputStream final : public ::paimon::OutputStream {
 public:
  PaimonBoltOutputStream(
      std::shared_ptr<bytedance::bolt::WriteFile> file,
      std::string uri)
      : file_(std::move(file)), uri_(std::move(uri)) {}

  ::paimon::Result<int32_t> Write(const char* buffer, uint32_t size) override {
    try {
      file_->append(std::string_view(buffer, size));
      pos_ += size;
      return static_cast<int32_t>(size);
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("write failed: ") + e.what());
    }
  }

  ::paimon::Status Flush() override {
    try {
      file_->flush();
      return ::paimon::Status::OK();
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("flush failed: ") + e.what());
    }
  }

  ::paimon::Result<int64_t> GetPos() const override {
    return pos_;
  }

  ::paimon::Result<std::string> GetUri() const override {
    return uri_;
  }

  ::paimon::Status Close() override {
    try {
      if (file_) {
        file_->close();
      }
      file_.reset();
      return ::paimon::Status::OK();
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("close failed: ") + e.what());
    }
  }

 private:
  std::shared_ptr<bytedance::bolt::WriteFile> file_;
  std::string uri_;
  int64_t pos_{0};
};

class PaimonBoltBasicFileStatus final : public ::paimon::BasicFileStatus {
 public:
  PaimonBoltBasicFileStatus(std::string path, bool isDir)
      : path_(std::move(path)), isDir_(isDir) {}

  bool IsDir() const override {
    return isDir_;
  }

  std::string GetPath() const override {
    return path_;
  }

 private:
  std::string path_;
  bool isDir_{false};
};

class PaimonBoltFileStatus final : public ::paimon::FileStatus {
 public:
  PaimonBoltFileStatus(
      std::string path,
      bool isDir,
      uint64_t len,
      int64_t modificationTimeMs)
      : path_(std::move(path)),
        isDir_(isDir),
        len_(len),
        modificationTimeMs_(modificationTimeMs) {}

  uint64_t GetLen() const override {
    return len_;
  }

  bool IsDir() const override {
    return isDir_;
  }

  std::string GetPath() const override {
    return path_;
  }

  int64_t GetModificationTime() const override {
    return modificationTimeMs_;
  }

 private:
  std::string path_;
  bool isDir_{false};
  uint64_t len_{0};
  int64_t modificationTimeMs_{0};
};

std::unordered_map<std::string, std::string> toUnordered(
    const std::map<std::string, std::string>& options) {
  std::unordered_map<std::string, std::string> out;
  out.reserve(options.size());
  for (const auto& [k, v] : options) {
    out.emplace(k, v);
  }
  return out;
}

void ensurePaimonFactoryRegistered() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    auto* factory =
        new bytedance::bolt::connector::paimon::PaimonBoltFileSystemFactory();
    ::paimon::FactoryCreator::GetInstance()->Register(
        factory->Identifier(), factory);
  });
}

} // namespace

PaimonBoltFileSystem::PaimonBoltFileSystem(
    std::map<std::string, std::string> options)
    : connectorProperties_(
          std::make_shared<bytedance::bolt::config::ConfigBase>(
              toUnordered(options),
              /*_mutable=*/false)) {}

PaimonBoltFileSystem::~PaimonBoltFileSystem() = default;

::paimon::Result<std::unique_ptr<::paimon::InputStream>>
PaimonBoltFileSystem::Open(const std::string& path) const {
  try {
    auto fs =
        bytedance::bolt::filesystems::getFileSystem(path, connectorProperties_);
    auto file = fs->openFileForRead(path, {});
    auto shared = std::shared_ptr<bytedance::bolt::ReadFile>(std::move(file));
    return std::make_unique<PaimonBoltInputStream>(std::move(shared), path);
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        "Open failed for " + path + ": " + e.what());
  }
}

::paimon::Result<std::unique_ptr<::paimon::OutputStream>>
PaimonBoltFileSystem::Create(const std::string& path, bool overwrite) const {
  try {
    auto fs =
        bytedance::bolt::filesystems::getFileSystem(path, connectorProperties_);
    if (fs->exists(path)) {
      if (!overwrite) {
        return ::paimon::Status::Invalid(
            "Do not allow overwrite, but the file already exists: " + path);
      }
      fs->remove(path);
    }

    bytedance::bolt::filesystems::FileOptions options;
    options.shouldCreateParentDirectories = true;
    options.shouldThrowOnFileAlreadyExists = false;
    auto file = fs->openFileForWrite(path, options);
    auto shared = std::shared_ptr<bytedance::bolt::WriteFile>(std::move(file));
    return std::make_unique<PaimonBoltOutputStream>(std::move(shared), path);
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        "Create failed for " + path + ": " + e.what());
  }
}

::paimon::Status PaimonBoltFileSystem::Mkdirs(const std::string& path) const {
  try {
    auto fs =
        bytedance::bolt::filesystems::getFileSystem(path, connectorProperties_);
    fs->mkdir(path);
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        "Mkdirs failed for " + path + ": " + e.what());
  }
}

::paimon::Status PaimonBoltFileSystem::Rename(
    const std::string& src,
    const std::string& dst) const {
  const auto srcPrefix = uriAuthorityPrefix(src);
  const auto dstPrefix = uriAuthorityPrefix(dst);
  if (!srcPrefix.empty() && !dstPrefix.empty() && srcPrefix != dstPrefix) {
    return ::paimon::Status::Invalid(
        "Rename across different HDFS authorities is not supported: " + src +
        " -> " + dst);
  }

  try {
    auto sourceFs =
        bytedance::bolt::filesystems::getFileSystem(src, connectorProperties_);
    auto destinationFs =
        bytedance::bolt::filesystems::getFileSystem(dst, connectorProperties_);
    if (sourceFs->name() != destinationFs->name()) {
      return ::paimon::Status::Invalid(
          "Rename across different Bolt filesystems is not supported: " + src +
          " -> " + dst);
    }
    sourceFs->rename(src, dst, /*overwrite=*/false);
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        "Rename failed for " + src + " -> " + dst + ": " + e.what());
  }
}

::paimon::Status PaimonBoltFileSystem::Delete(
    const std::string& path,
    bool recursive) const {
  try {
    auto fs =
        bytedance::bolt::filesystems::getFileSystem(path, connectorProperties_);
    if (fs->fileInfo(path).isDirectory) {
      if (!recursive) {
        return ::paimon::Status::Invalid(
            "Non-recursive directory delete is not supported for " + path);
      }
      fs->rmdir(path);
    } else {
      fs->remove(path);
    }
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        "Delete failed for " + path + ": " + e.what());
  }
}

::paimon::Result<std::unique_ptr<::paimon::FileStatus>>
PaimonBoltFileSystem::GetFileStatus(const std::string& path) const {
  try {
    auto fs =
        bytedance::bolt::filesystems::getFileSystem(path, connectorProperties_);
    const auto info = fs->fileInfo(path);
    return std::make_unique<PaimonBoltFileStatus>(
        path, info.isDirectory, info.size, info.modificationTimeMs);
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        "GetFileStatus failed for " + path + ": " + e.what());
  }
}

::paimon::Status PaimonBoltFileSystem::ListDir(
    const std::string& directory,
    std::vector<std::unique_ptr<::paimon::BasicFileStatus>>* file_status_list)
    const {
  try {
    auto fs = bytedance::bolt::filesystems::getFileSystem(
        directory, connectorProperties_);
    if (!fs->exists(directory)) {
      return ::paimon::Status::OK();
    }
    if (!fs->fileInfo(directory).isDirectory) {
      return ::paimon::Status::IOError(
          "ListDir target is not a directory: " + directory);
    }

    auto entries = fs->list(directory);
    file_status_list->reserve(file_status_list->size() + entries.size());
    for (const auto& entry : entries) {
      const std::string full = joinDirAndBasename(directory, entry);
      const bool isDir = fs->fileInfo(full).isDirectory;
      file_status_list->emplace_back(
          std::make_unique<PaimonBoltBasicFileStatus>(full, isDir));
    }
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        "ListDir failed for " + directory + ": " + e.what());
  }
}

::paimon::Status PaimonBoltFileSystem::ListFileStatus(
    const std::string& path,
    std::vector<std::unique_ptr<::paimon::FileStatus>>* file_status_list)
    const {
  try {
    auto fs =
        bytedance::bolt::filesystems::getFileSystem(path, connectorProperties_);
    if (!fs->exists(path)) {
      return ::paimon::Status::OK();
    }
    const auto info = fs->fileInfo(path);
    if (!info.isDirectory) {
      file_status_list->emplace_back(std::make_unique<PaimonBoltFileStatus>(
          path, info.isDirectory, info.size, info.modificationTimeMs));
      return ::paimon::Status::OK();
    }

    auto entries = fs->list(path);
    file_status_list->reserve(file_status_list->size() + entries.size());
    for (const auto& entry : entries) {
      const std::string full = joinDirAndBasename(path, entry);
      const auto entryInfo = fs->fileInfo(full);
      file_status_list->emplace_back(std::make_unique<PaimonBoltFileStatus>(
          full,
          entryInfo.isDirectory,
          entryInfo.size,
          entryInfo.modificationTimeMs));
    }
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        "ListFileStatus failed for " + path + ": " + e.what());
  }
}

::paimon::Result<bool> PaimonBoltFileSystem::Exists(
    const std::string& path) const {
  try {
    auto fs =
        bytedance::bolt::filesystems::getFileSystem(path, connectorProperties_);
    return fs->exists(path);
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        "Exists failed for " + path + ": " + e.what());
  }
}

const char* PaimonBoltFileSystemFactory::Identifier() const {
  return kIdentifier.data();
}

::paimon::Result<std::unique_ptr<::paimon::FileSystem>>
PaimonBoltFileSystemFactory::Create(
    const std::string& /*path*/,
    const std::map<std::string, std::string>& options) const {
  return std::make_unique<PaimonBoltFileSystem>(options);
}

void EnsurePaimonBoltFileSystemRegistered() {
  ensurePaimonFactoryRegistered();
}

} // namespace bytedance::bolt::connector::paimon
