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
#include "bolt/common/file/File.h"

namespace bytedance::bolt {

class TosReadFile final : public ReadFile {
 public:
  explicit TosReadFile(
      const std::shared_ptr<VolcengineTos::TosClientV2>& tosClient,
      const std::string_view& bucketName,
      const std::string_view& objectName);

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final;

  std::string pread(uint64_t offset, uint64_t length) const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const final;

  uint64_t size() const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final;

  std::string getName() const final {
    return objectName_;
  }

  uint64_t getNaturalReadSize() const final {
    return 72 << 20;
  }

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* pos) const;
  void checkFileReadParameters(uint64_t offset, uint64_t length) const;
  std::shared_ptr<VolcengineTos::TosClientV2> tosClient_;
  const std::string bucketName_;
  const std::string objectName_;
  uint64_t fileSize_;
};
} // namespace bytedance::bolt
