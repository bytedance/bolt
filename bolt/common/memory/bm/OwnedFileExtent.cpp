#include "bolt/common/memory/bm/OwnedFileExtent.h"

#include <glog/logging.h>

namespace bytedance::bolt::memory::bm {

OwnedFileExtent::OwnedFileExtent(
    FileExtent extent,
    std::weak_ptr<FileBlockAllocator> allocator)
    : extent_(extent), allocator_(std::move(allocator)), valid_(true) {}

OwnedFileExtent::~OwnedFileExtent() noexcept {
  ResetNoexcept("OwnedFileExtent::~OwnedFileExtent");
}

OwnedFileExtent::OwnedFileExtent(OwnedFileExtent&& other) noexcept
    : extent_(other.extent_),
      allocator_(std::move(other.allocator_)),
      valid_(other.valid_) {
  other.valid_ = false;
}

OwnedFileExtent& OwnedFileExtent::operator=(OwnedFileExtent&& other) noexcept {
  if (this != &other) {
    ResetNoexcept("OwnedFileExtent::operator=");
    extent_ = other.extent_;
    allocator_ = std::move(other.allocator_);
    valid_ = other.valid_;
    other.valid_ = false;
  }
  return *this;
}

const FileExtent& OwnedFileExtent::extent() const {
  return extent_;
}

bool OwnedFileExtent::valid() const {
  return valid_;
}

void OwnedFileExtent::FreeOrFatal(std::string_view context) noexcept {
  ResetNoexcept(context);
}

void OwnedFileExtent::ResetNoexcept(std::string_view context) noexcept {
  if (!valid_) {
    return;
  }

  auto allocator = allocator_.lock();
  if (!allocator) {
    LOG(FATAL) << "BM file extent allocator expired while freeing extent in "
               << context << ", extent_id=" << extent_.id
               << ", fd=" << extent_.fd << ", offset=" << extent_.offset
               << ", requested_size=" << extent_.requested_size;
  }

  const auto result = allocator->Free(extent_);
  valid_ = false;
  if (!result.ok()) {
    LOG(FATAL) << "BM file extent free failed in " << context
               << ", extent_id=" << extent_.id << ", fd=" << extent_.fd
               << ", offset=" << extent_.offset
               << ", requested_size=" << extent_.requested_size
               << ", file_error=" << static_cast<int>(result.error)
               << ", native_error=" << result.native_error_code;
  }
}

} // namespace bytedance::bolt::memory::bm
