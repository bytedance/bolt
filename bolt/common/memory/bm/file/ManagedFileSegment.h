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

#include "bolt/common/memory/bm/file/FileSegmentAllocator.h"

#include <memory>
#include <string_view>

namespace bytedance::bolt::memory::bm {

class ManagedFileSegment {
 public:
  ManagedFileSegment() = default;
  ManagedFileSegment(
      FileSegment segment,
      std::weak_ptr<FileSegmentAllocator> allocator);
  ~ManagedFileSegment() noexcept;

  ManagedFileSegment(ManagedFileSegment&& other) noexcept;
  ManagedFileSegment& operator=(ManagedFileSegment&& other) noexcept;

  ManagedFileSegment(const ManagedFileSegment&) = delete;
  ManagedFileSegment& operator=(const ManagedFileSegment&) = delete;

  const FileSegment& segment() const;
  bool valid() const;
  void FreeOrFatal(std::string_view context) noexcept;

 private:
  void ResetNoexcept(std::string_view context) noexcept;

  FileSegment segment_;
  std::weak_ptr<FileSegmentAllocator> allocator_;
  bool valid_{false};
};

} // namespace bytedance::bolt::memory::bm
