#include "bolt/common/memory/bm/OwnedFileSegment.h"

#include <glog/logging.h>

namespace bytedance::bolt::memory::bm {

OwnedFileSegment::OwnedFileSegment(
    FileSegment segment,
    std::weak_ptr<FileSegmentAllocator> allocator)
    : segment_(segment), allocator_(std::move(allocator)), valid_(true) {}

OwnedFileSegment::~OwnedFileSegment() noexcept {
  ResetNoexcept("OwnedFileSegment::~OwnedFileSegment");
}

OwnedFileSegment::OwnedFileSegment(OwnedFileSegment&& other) noexcept
    : segment_(other.segment_),
      allocator_(std::move(other.allocator_)),
      valid_(other.valid_) {
  other.valid_ = false;
}

OwnedFileSegment& OwnedFileSegment::operator=(OwnedFileSegment&& other) noexcept {
  if (this != &other) {
    ResetNoexcept("OwnedFileSegment::operator=");
    segment_ = other.segment_;
    allocator_ = std::move(other.allocator_);
    valid_ = other.valid_;
    other.valid_ = false;
  }
  return *this;
}

const FileSegment& OwnedFileSegment::segment() const {
  return segment_;
}

bool OwnedFileSegment::valid() const {
  return valid_;
}

void OwnedFileSegment::FreeOrFatal(std::string_view context) noexcept {
  ResetNoexcept(context);
}

void OwnedFileSegment::ResetNoexcept(std::string_view context) noexcept {
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
