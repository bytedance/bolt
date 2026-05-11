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

#include "bolt/common/file/File.h"
#include "bolt/external/hdfs/hdfs.h"
namespace bytedance::bolt {

namespace filesystems::arrow::io::internal {
class LibHdfsShim;
}

/**
 * Implementation of hdfs read file.
 */
class HdfsReadFile final : public ReadFile {
 public:
  explicit HdfsReadFile(
      filesystems::arrow::io::internal::LibHdfsShim* driver,
      hdfsFS hdfs,
      std::string_view path,
      int bufferSize = 0);
  ~HdfsReadFile() override;

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final;

  std::string pread(uint64_t offset, uint64_t length) const final;

  uint64_t size() const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final;

  std::string getName() const final;

  uint64_t getNaturalReadSize() const final {
    return 72 << 20;
  }

 private:
  void checkFileReadParameters(uint64_t offset, uint64_t length) const;

  class Impl;
  std::unique_ptr<Impl> pImpl;
};

} // namespace bytedance::bolt
