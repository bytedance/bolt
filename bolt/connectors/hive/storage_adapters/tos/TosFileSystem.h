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

#include <TosClientV2.h>
#include <Type.h>
#include <folly/Synchronized.h>
#include "bolt/common/caching/SimpleLRUCache.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/storage_adapters/tos/TosConfig.h"

namespace bytedance::bolt::filesystems {

/// TOS object reads and sequential writes through the native C++ SDK.
class TosFileSystem : public FileSystem {
 private:
  using TosBucketTypeCache =
      folly::Synchronized<SimpleLRUCache<std::string, BucketType>>;

  TosBucketTypeCache bucketTypeCache_;

 public:
  explicit TosFileSystem(std::shared_ptr<const config::ConfigBase> config);

  std::string name() const override;

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options = {}) override;

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options = {}) override;

  void remove(std::string_view path) override {
    BOLT_UNSUPPORTED("Does not support removing files from Tos");
  }

  void rename(
      std::string_view path,
      std::string_view newPath,
      bool overWrite = false) override {
    BOLT_UNSUPPORTED("rename for Tos not implemented");
  }

  bool exists(std::string_view path) override {
    BOLT_UNSUPPORTED("exists for Tos not implemented");
  }

  std::vector<std::string> list(std::string_view path) override {
    BOLT_UNSUPPORTED("list for Tos not implemented");
  }

  void mkdir(std::string_view path) override {
    BOLT_UNSUPPORTED("mkdir for Tos not implemented");
  }

  void rmdir(std::string_view path) override {
    BOLT_UNSUPPORTED("rmdir for Tos not implemented");
  }

  /// Splits a tos://bucket/object URI without decoding the object key.
  static std::pair<std::string_view, std::string_view> parsePath(
      std::string_view path);

  static bool isTosFile(std::string_view filename);
};
} // namespace bytedance::bolt::filesystems
