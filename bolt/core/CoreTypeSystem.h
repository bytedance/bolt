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

#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/core/Metaprogramming.h"
#include "bolt/functions/UDFOutputString.h"
#include "bolt/type/Type.h"
#include "bolt/type/Variant.h"
namespace bytedance::bolt::core {

// A simple & efficient container/target for user strings
struct StringWriter : public UDFOutputString {
  StringWriter() noexcept : storage_{} {
    setData(storage_.data());
  }

  /* implicit */ StringWriter(StringView /*value*/) {
    BOLT_NYI();
  }

  void setEmpty() {
    BOLT_FAIL("setEmpty is not implemented");
  }

  void setNoCopy(StringView /*value*/) {
    BOLT_FAIL("setNoCopy is not implemented");
  }

  StringWriter(const StringWriter& rh) : storage_{rh.storage_} {
    setData(storage_.data());
    setSize(rh.size());
    setCapacity(rh.capacity());
  }

  StringWriter(StringWriter&& rh) noexcept : storage_{std::move(rh.storage_)} {
    setData(storage_.data());
    setSize(rh.size());
    setCapacity(rh.capacity());
  }

  StringWriter& operator=(const StringWriter& rh) {
    storage_ = rh.storage_;
    reserve(rh.capacity());
    resize(rh.size());
    return *this;
  }

  template <typename T>
  void operator+=(const T& input) {
    append(input);
  }

  void operator+=(const char* input) {
    append(std::string_view(input));
  }

  template <typename T>
  void append(const T& input) {
    auto oldSize = size();
    resize(this->size() + input.size());
    if (input.size() != 0) {
      DCHECK(data());
      DCHECK(input.data());
      std::memcpy(data() + oldSize, input.data(), input.size());
    }
  }

  void append(const char* input) {
    append(std::string_view(input));
  }

  template <typename T>
  void copy_from(const T& input) {
    resize(0);
    append(input);
  }

  void copy_from(const char* input) {
    append(std::string_view(input));
  }

  StringWriter& operator=(StringWriter&& rh) noexcept {
    storage_ = std::move(rh.storage_);
    setData(storage_.data());
    setSize(rh.size());
    setCapacity(rh.capacity());
    return *this;
  }

  void reserve(size_t size) override {
    // Resizing the storage not StringWriter size.
    // This allow us to write directly write into storage_.data() and assuring
    // what we wrote won't be overwritten on future resize calls.
    storage_.resize(size);
    setData(storage_.data());
    setCapacity(size);
  }

  /// Not called by the UDF but should be called internally at the end of the
  /// UDF call
  void finalize() {
    storage_.resize(size());
  }

  operator StringView() const {
    return StringView(data(), size());
  }

 private:
  folly::fbstring storage_;
};

} // namespace bytedance::bolt::core
