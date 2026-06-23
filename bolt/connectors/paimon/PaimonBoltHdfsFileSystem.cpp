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

#include "bolt/connectors/paimon/PaimonBoltHdfsFileSystem.h"

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
#include "bolt/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include "bolt/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"

#include "paimon/factories/factory_creator.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace bytedance::bolt::connector::paimon {

namespace {

constexpr std::string_view kHdfsScheme{"hdfs://"};
constexpr std::string_view kIdentifier{"bolt_hdfs"};

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

class PaimonBoltHdfsInputStream final : public ::paimon::InputStream {
 public:
  PaimonBoltHdfsInputStream(
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

class PaimonBoltHdfsOutputStream final : public ::paimon::OutputStream {
 public:
  PaimonBoltHdfsOutputStream(
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

class PaimonBoltHdfsBasicFileStatus final : public ::paimon::BasicFileStatus {
 public:
  PaimonBoltHdfsBasicFileStatus(std::string path, bool isDir)
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

class PaimonBoltHdfsFileStatus final : public ::paimon::FileStatus {
 public:
  PaimonBoltHdfsFileStatus(
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
    auto* factory = new bytedance::bolt::connector::paimon::
        PaimonBoltHdfsFileSystemFactory();
    ::paimon::FactoryCreator::GetInstance()->Register(
        factory->Identifier(), factory);
  });
}

} // namespace

PaimonBoltHdfsFileSystem::PaimonBoltHdfsFileSystem(
    std::map<std::string, std::string> options)
    : connectorProperties_(
          std::make_shared<bytedance::bolt::config::ConfigBase>(
              toUnordered(options),
              /*_mutable=*/false)) {}

PaimonBoltHdfsFileSystem::~PaimonBoltHdfsFileSystem() = default;

::paimon::Result<std::unique_ptr<::paimon::InputStream>>
PaimonBoltHdfsFileSystem::Open(const std::string& path) const {
  // Construct an HDFS filesystem instance using the same generator as Hive.
  auto fs = bytedance::bolt::filesystems::hdfsFileSystemGenerator()(
      connectorProperties_, path);
  try {
    auto file = fs->openFileForRead(path, {});
    auto shared = std::shared_ptr<bytedance::bolt::ReadFile>(std::move(file));
    return std::make_unique<PaimonBoltHdfsInputStream>(std::move(shared), path);
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(std::string("Open failed: ") + e.what());
  }
}

::paimon::Result<std::unique_ptr<::paimon::OutputStream>>
PaimonBoltHdfsFileSystem::Create(const std::string& path, bool overwrite)
    const {
  auto fs = bytedance::bolt::filesystems::hdfsFileSystemGenerator()(
      connectorProperties_, path);
  try {
    if (fs->exists(path)) {
      if (!overwrite) {
        return ::paimon::Status::Invalid(
            "do not allow overwrite, but the file already exists");
      }
      fs->remove(path);
    }

    bytedance::bolt::filesystems::FileOptions options;
    options.shouldCreateParentDirectories = true;
    options.shouldThrowOnFileAlreadyExists = false;
    auto file = fs->openFileForWrite(path, options);
    auto shared = std::shared_ptr<bytedance::bolt::WriteFile>(std::move(file));
    return std::make_unique<PaimonBoltHdfsOutputStream>(
        std::move(shared), path);
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(std::string("Create failed: ") + e.what());
  }
}

::paimon::Status PaimonBoltHdfsFileSystem::Mkdirs(
    const std::string& path) const {
  auto fs = bytedance::bolt::filesystems::hdfsFileSystemGenerator()(
      connectorProperties_, path);
  try {
    fs->mkdir(path);
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(std::string("Mkdirs failed: ") + e.what());
  }
}

::paimon::Status PaimonBoltHdfsFileSystem::Rename(
    const std::string& src,
    const std::string& dst) const {
  const auto srcPrefix = uriAuthorityPrefix(src);
  const auto dstPrefix = uriAuthorityPrefix(dst);
  if (!srcPrefix.empty() && !dstPrefix.empty() && srcPrefix != dstPrefix) {
    return ::paimon::Status::Invalid(
        "Rename across different HDFS authorities is not supported");
  }

  auto fs = bytedance::bolt::filesystems::hdfsFileSystemGenerator()(
      connectorProperties_, src);
  try {
    fs->rename(src, dst, /*overwrite=*/false);
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(std::string("Rename failed: ") + e.what());
  }
}

::paimon::Status PaimonBoltHdfsFileSystem::Delete(
    const std::string& path,
    bool recursive) const {
  auto fs = bytedance::bolt::filesystems::hdfsFileSystemGenerator()(
      connectorProperties_, path);
  try {
    auto* hdfsFs =
        dynamic_cast<bytedance::bolt::filesystems::HdfsFileSystem*>(fs.get());
    if (!hdfsFs) {
      return ::paimon::Status::NotImplemented(
          "hdfs:// path did not resolve to HdfsFileSystem");
    }

    if (hdfsFs->stat(path).isDir) {
      if (!recursive) {
        return ::paimon::Status::Invalid(
            "non-recursive directory delete is not supported");
      }
      fs->rmdir(path);
    } else {
      fs->remove(path);
    }
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(std::string("Delete failed: ") + e.what());
  }
}

::paimon::Result<std::unique_ptr<::paimon::FileStatus>>
PaimonBoltHdfsFileSystem::GetFileStatus(const std::string& path) const {
  auto fs = bytedance::bolt::filesystems::hdfsFileSystemGenerator()(
      connectorProperties_, path);
  try {
    auto* hdfsFs =
        dynamic_cast<bytedance::bolt::filesystems::HdfsFileSystem*>(fs.get());
    if (!hdfsFs) {
      return ::paimon::Status::NotImplemented(
          "hdfs:// path did not resolve to HdfsFileSystem");
    }
    auto info = hdfsFs->stat(path);
    return std::make_unique<PaimonBoltHdfsFileStatus>(
        path, info.isDir, info.size, info.modificationTimeMs);
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        std::string("GetFileStatus failed: ") + e.what());
  }
}

::paimon::Status PaimonBoltHdfsFileSystem::ListDir(
    const std::string& directory,
    std::vector<std::unique_ptr<::paimon::BasicFileStatus>>* file_status_list)
    const {
  auto fs = bytedance::bolt::filesystems::hdfsFileSystemGenerator()(
      connectorProperties_, directory);
  try {
    if (!fs->exists(directory)) {
      return ::paimon::Status::OK();
    }

    auto* hdfsFs =
        dynamic_cast<bytedance::bolt::filesystems::HdfsFileSystem*>(fs.get());
    if (!hdfsFs) {
      return ::paimon::Status::NotImplemented(
          "hdfs:// path did not resolve to HdfsFileSystem");
    }
    if (!hdfsFs->stat(directory).isDir) {
      return ::paimon::Status::IOError(
          "ListDir target exists and is not a directory");
    }

    auto entries = fs->list(directory);
    file_status_list->reserve(file_status_list->size() + entries.size());
    for (const auto& entry : entries) {
      const std::string full = joinDirAndBasename(directory, entry);
      const bool isDir = hdfsFs->stat(full).isDir;
      file_status_list->emplace_back(
          std::make_unique<PaimonBoltHdfsBasicFileStatus>(full, isDir));
    }
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        std::string("ListDir failed: ") + e.what());
  }
}

::paimon::Status PaimonBoltHdfsFileSystem::ListFileStatus(
    const std::string& path,
    std::vector<std::unique_ptr<::paimon::FileStatus>>* file_status_list)
    const {
  auto fs = bytedance::bolt::filesystems::hdfsFileSystemGenerator()(
      connectorProperties_, path);
  try {
    if (!fs->exists(path)) {
      return ::paimon::Status::OK();
    }

    auto* hdfsFs =
        dynamic_cast<bytedance::bolt::filesystems::HdfsFileSystem*>(fs.get());
    if (!hdfsFs) {
      return ::paimon::Status::NotImplemented(
          "hdfs:// path did not resolve to HdfsFileSystem");
    }

    if (!hdfsFs->stat(path).isDir) {
      auto info = hdfsFs->stat(path);
      file_status_list->emplace_back(std::make_unique<PaimonBoltHdfsFileStatus>(
          path, info.isDir, info.size, info.modificationTimeMs));
      return ::paimon::Status::OK();
    }

    auto entries = fs->list(path);
    file_status_list->reserve(file_status_list->size() + entries.size());
    for (const auto& entry : entries) {
      const std::string full = joinDirAndBasename(path, entry);
      auto info = hdfsFs->stat(full);
      file_status_list->emplace_back(std::make_unique<PaimonBoltHdfsFileStatus>(
          full, info.isDir, info.size, info.modificationTimeMs));
    }
    return ::paimon::Status::OK();
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(
        std::string("ListFileStatus failed: ") + e.what());
  }
}

::paimon::Result<bool> PaimonBoltHdfsFileSystem::Exists(
    const std::string& path) const {
  auto fs = bytedance::bolt::filesystems::hdfsFileSystemGenerator()(
      connectorProperties_, path);
  try {
    return fs->exists(path);
  } catch (const std::exception& e) {
    return ::paimon::Status::IOError(std::string("Exists failed: ") + e.what());
  }
}

const char* PaimonBoltHdfsFileSystemFactory::Identifier() const {
  return kIdentifier.data();
}

::paimon::Result<std::unique_ptr<::paimon::FileSystem>>
PaimonBoltHdfsFileSystemFactory::Create(
    const std::string& /*path*/,
    const std::map<std::string, std::string>& options) const {
  return std::make_unique<PaimonBoltHdfsFileSystem>(options);
}

void EnsurePaimonBoltHdfsFileSystemRegistered() {
  static std::once_flag flag;
  std::call_once(
      flag, []() { bytedance::bolt::filesystems::registerHdfsFileSystem(); });
  ensurePaimonFactoryRegistered();
}

} // namespace bytedance::bolt::connector::paimon
