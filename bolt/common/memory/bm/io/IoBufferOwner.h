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

#include <memory>
#include <utility>

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

} // namespace bytedance::bolt::memory::bm
