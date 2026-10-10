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

#include "bolt/common/memory/bm/file/ManagedFileSegment.h"

#include <glog/logging.h>

namespace bytedance::bolt::memory::bm {

ManagedFileSegment::ManagedFileSegment(
    FileSegment segment,
    std::weak_ptr<FileSegmentAllocator> allocator)
    : segment_(segment), allocator_(std::move(allocator)), valid_(true) {}

ManagedFileSegment::~ManagedFileSegment() noexcept {
  ResetNoexcept("ManagedFileSegment::~ManagedFileSegment");
}

ManagedFileSegment::ManagedFileSegment(ManagedFileSegment&& other) noexcept
    : segment_(other.segment_),
      allocator_(std::move(other.allocator_)),
      valid_(other.valid_) {
  other.valid_ = false;
}

ManagedFileSegment& ManagedFileSegment::operator=(
    ManagedFileSegment&& other) noexcept {
  if (this != &other) {
    ResetNoexcept("ManagedFileSegment::operator=");
    segment_ = other.segment_;
    allocator_ = std::move(other.allocator_);
    valid_ = other.valid_;
    other.valid_ = false;
  }
  return *this;
}

const FileSegment& ManagedFileSegment::segment() const {
  return segment_;
}

bool ManagedFileSegment::valid() const {
  return valid_;
}

void ManagedFileSegment::FreeOrFatal(std::string_view context) noexcept {
  ResetNoexcept(context);
}

void ManagedFileSegment::ResetNoexcept(std::string_view context) noexcept {
  if (!valid_) {
    return;
  }

  auto allocator = allocator_.lock();
  if (!allocator) {
    LOG(FATAL) << "BM file segment allocator expired while freeing segment in "
               << context << ", segment_id=" << segment_.id
               << ", fd=" << segment_.fd << ", offset=" << segment_.offset
               << ", requested_size=" << segment_.requested_size;
  }

  const auto result = allocator->Free(segment_);
  valid_ = false;
  if (!result.ok()) {
    LOG(FATAL) << "BM file segment free failed in " << context
               << ", segment_id=" << segment_.id << ", fd=" << segment_.fd
               << ", offset=" << segment_.offset
               << ", requested_size=" << segment_.requested_size
               << ", file_error=" << static_cast<int>(result.error)
               << ", native_error=" << result.native_error_code;
  }
}

} // namespace bytedance::bolt::memory::bm
