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

#pragma once

#include <google/cloud/storage/client.h>
#include "bolt/common/file/File.h"

namespace bytedance::bolt::filesystems {

/**
 * Implementation of gcs read file.
 */
class GcsReadFile : public ReadFile {
 public:
  GcsReadFile(
      const std::string& path,
      std::shared_ptr<::google::cloud::storage::Client> client);

  ~GcsReadFile() override;

  void initialize();

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer)
      const override;

  std::string pread(uint64_t offset, uint64_t length) const override;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override;

  uint64_t size() const override;

  uint64_t memoryUsage() const override;

  bool shouldCoalesce() const final {
    return false;
  }

  std::string getName() const override;

  uint64_t getNaturalReadSize() const override;

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace bytedance::bolt::filesystems
