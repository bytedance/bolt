#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/MemoryPool.h"

namespace bytedance::bolt::memory::bm {

class UniqueBufferOwner {
 public:
  UniqueBufferOwner() = default;

  template <typename Deleter>
  UniqueBufferOwner(char* data, Deleter deleter)
      : data_(data),
        deleter_(std::make_unique<Model<Deleter>>(std::move(deleter))) {
    static_assert(
        noexcept(std::declval<Deleter&>()(std::declval<char*>())),
        "IoBuffer deleter must be noexcept");
  }

  ~UniqueBufferOwner() {
    reset();
  }

  UniqueBufferOwner(UniqueBufferOwner&& other) noexcept
      : data_(other.data_), deleter_(std::move(other.deleter_)) {
    other.data_ = nullptr;
  }

  UniqueBufferOwner& operator=(UniqueBufferOwner&& other) noexcept {
    if (this != &other) {
      reset();
      data_ = other.data_;
      deleter_ = std::move(other.deleter_);
      other.data_ = nullptr;
    }
    return *this;
  }

  UniqueBufferOwner(const UniqueBufferOwner&) = delete;
  UniqueBufferOwner& operator=(const UniqueBufferOwner&) = delete;

  bool owns() const {
    return data_ != nullptr && deleter_ != nullptr;
  }

  void reset() noexcept {
    if (!owns()) {
      return;
    }
    auto* data = data_;
    data_ = nullptr;
    deleter_->destroy(data);
    deleter_.reset();
  }

 private:
  struct Concept {
    virtual ~Concept() = default;
    virtual void destroy(char* data) noexcept = 0;
  };

  template <typename Deleter>
  struct Model final : Concept {
    explicit Model(Deleter deleter) : deleter_(std::move(deleter)) {}

    void destroy(char* data) noexcept override {
      deleter_(data);
    }

    Deleter deleter_;
  };

  char* data_{nullptr};
  std::unique_ptr<Concept> deleter_;
};

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

struct IoRequest {
  IoOpcode opcode{IoOpcode::Read};
  IoPriority priority{IoPriority::Medium};
  int fd{-1};
  uint64_t fileOffset{0};
  IoBuffer buffer;
};

} // namespace bytedance::bolt::memory::bm
