#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/io/IoBufferOwner.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <utility>

namespace bytedance::bolt::memory::bm {

struct MemoryPoolBufferDeleter {
  MemoryPool* pool{nullptr};
  int64_t size{0};
  std::optional<uint8_t> alignment;
  bool needRecordFree{true};

  void operator()(char* data) const noexcept {
    if (data != nullptr) {
      pool->free(data, size, alignment, needRecordFree);
    }
  }
};

class IoBuffer {
 public:
  IoBuffer() = default;

  IoBuffer(
      std::unique_ptr<char[]> data,
      size_t size,
      size_t offset,
      size_t length)
      : data_(data.release()),
        size_(size),
        offset_(offset),
        length_(length),
        owner_(data_, [](char* p) noexcept { delete[] p; }) {}

  IoBuffer(IoBuffer&&) noexcept = default;
  IoBuffer& operator=(IoBuffer&&) noexcept = default;

  IoBuffer(const IoBuffer&) = delete;
  IoBuffer& operator=(const IoBuffer&) = delete;

  static IoBuffer allocateFromPool(
      MemoryPool* pool,
      size_t size,
      std::optional<uint8_t> alignment = std::nullopt) {
    BOLT_CHECK_NOT_NULL(pool);
    std::optional<uint32_t> allocateAlignment;
    if (alignment.has_value()) {
      allocateAlignment = *alignment;
    }
    auto* data = static_cast<char*>(pool->allocate(size, allocateAlignment));
    return fromOwned(
        data,
        size,
        0,
        size,
        MemoryPoolBufferDeleter{
            pool, static_cast<int64_t>(size), alignment, true});
  }

  static IoBuffer allocateFromMalloc(size_t size) {
    constexpr size_t kAlignment = 4096;
    BOLT_CHECK_GT(size, 0);
    const auto allocationSize =
        ((size + kAlignment - 1) / kAlignment) * kAlignment;
    auto* data = static_cast<char*>(std::malloc(allocationSize));
    if (data == nullptr) {
      BOLT_FAIL("BM IoBuffer malloc failed, size={}", size);
    }
    return fromOwned(
        data,
        allocationSize,
        0,
        size,
        [](char* p) noexcept { std::free(p); });
  }

  template <typename Deleter>
  static IoBuffer fromOwned(
      char* data,
      size_t size,
      size_t offset,
      size_t length,
      Deleter deleter) {
    return IoBuffer{
        data,
        size,
        offset,
        length,
        UniqueBufferOwner{data, std::move(deleter)}};
  }

  char* data() const {
    return data_;
  }

  char* ioData() const {
    return data_ + offset_;
  }

  size_t size() const {
    return size_;
  }

  size_t offset() const {
    return offset_;
  }

  size_t length() const {
    return length_;
  }

  void setLength(size_t length) {
    BOLT_CHECK_LE(length, size_ - offset_);
    length_ = length;
  }

  bool valid() const {
    return data_ != nullptr && owner_.owns() && offset_ <= size_ &&
        length_ <= size_ - offset_;
  }

 private:
  IoBuffer(
      char* data,
      size_t size,
      size_t offset,
      size_t length,
      UniqueBufferOwner owner)
      : data_(data),
        size_(size),
        offset_(offset),
        length_(length),
        owner_(std::move(owner)) {}

  char* data_{nullptr};
  size_t size_{0};
  size_t offset_{0};
  size_t length_{0};
  UniqueBufferOwner owner_;
};

} // namespace bytedance::bolt::memory::bm
