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

#pragma once

#include <map>
#include <memory>

#include "bolt/common/config/Config.h"

#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"

namespace bytedance::bolt::connector::paimon {

// Paimon filesystem implementation backed by Bolt's registered hdfs://
// filesystem.
//
// This class expects hdfs:// URIs. It resolves the underlying Bolt filesystem
// via `bytedance::bolt::filesystems::getFileSystem(hdfsUri,
// connectorProperties)`.
class PaimonBoltHdfsFileSystem final : public ::paimon::FileSystem {
 public:
  explicit PaimonBoltHdfsFileSystem(std::map<std::string, std::string> options);
  ~PaimonBoltHdfsFileSystem() override;

  ::paimon::Result<std::unique_ptr<::paimon::InputStream>> Open(
      const std::string& path) const override;

  ::paimon::Result<std::unique_ptr<::paimon::OutputStream>> Create(
      const std::string& path,
      bool overwrite) const override;

  ::paimon::Status Mkdirs(const std::string& path) const override;
  ::paimon::Status Rename(const std::string& src, const std::string& dst)
      const override;
  ::paimon::Status Delete(const std::string& path, bool recursive)
      const override;

  ::paimon::Result<std::unique_ptr<::paimon::FileStatus>> GetFileStatus(
      const std::string& path) const override;

  ::paimon::Status ListDir(
      const std::string& directory,
      std::vector<std::unique_ptr<::paimon::BasicFileStatus>>* file_status_list)
      const override;

  ::paimon::Status ListFileStatus(
      const std::string& path,
      std::vector<std::unique_ptr<::paimon::FileStatus>>* file_status_list)
      const override;

  ::paimon::Result<bool> Exists(const std::string& path) const override;

 private:
  std::shared_ptr<const bytedance::bolt::config::ConfigBase>
      connectorProperties_;
};

class PaimonBoltHdfsFileSystemFactory final
    : public ::paimon::FileSystemFactory {
 public:
  ~PaimonBoltHdfsFileSystemFactory() override = default;

  const char* Identifier() const override;

  ::paimon::Result<std::unique_ptr<::paimon::FileSystem>> Create(
      const std::string& path,
      const std::map<std::string, std::string>& options) const override;
};

// Ensures that:
// 1) Bolt HDFS filesystem backend is registered (so bolt can resolve hdfs://
// URIs), and 2) paimon-cpp FileSystemFactory identifier "bolt_hdfs" is
// registered.
//
// Safe to call multiple times.
void EnsurePaimonBoltHdfsFileSystemRegistered();

} // namespace bytedance::bolt::connector::paimon
