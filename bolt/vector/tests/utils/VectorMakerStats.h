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

#include <algorithm>
#include <optional>
#include <unordered_set>

#include "bolt/type/Type.h"
#include "bolt/vector/SimpleVector.h"
namespace bytedance::bolt::test {
template <typename T>
struct EvalTypeHelper {
  using Type = typename CppToType<T>::NativeType;
};

template <typename T>
struct EvalTypeHelper<std::optional<T>> {
  using Type = typename EvalTypeHelper<T>::Type;
};

template <>
struct EvalTypeHelper<uint8_t> {
  using Type = uint8_t;
};

template <>
struct EvalTypeHelper<uint16_t> {
  using Type = uint16_t;
};

template <>
struct EvalTypeHelper<uint32_t> {
  using Type = uint32_t;
};

template <>
struct EvalTypeHelper<uint64_t> {
  using Type = uint64_t;
};

template <>
struct EvalTypeHelper<uint128_t> {
  using Type = uint128_t;
};

template <typename T>
using EvalType = typename EvalTypeHelper<T>::Type;

template <typename T>
struct VectorMakerStatsHash : std::hash<T> {};

#if defined(BOLT_CLANG_LIBSTDCXX_INT128_COMPAT)
template <>
struct VectorMakerStatsHash<int128_t> {
  size_t operator()(int128_t value) const noexcept {
    return HugeInt::hash(value);
  }
};

template <>
struct VectorMakerStatsHash<uint128_t> {
  size_t operator()(uint128_t value) const noexcept {
    return HugeInt::hash(value);
  }
};
#endif

// Struct that caries metadata about a vector of nullable elements.
template <typename T>
class VectorMakerStats {
 public:
  void addElement(const T& val) {
    distinctSet_.insert(val);
  }

  size_t distinctCount() const {
    return distinctSet_.size();
  }

  SimpleVectorStats<T> asSimpleVectorStats() {
    return {min, max};
  }

  std::optional<T> min;
  std::optional<T> max;
  size_t nullCount{0};
  bool isSorted{false};

 private:
  std::unordered_set<T, VectorMakerStatsHash<T>> distinctSet_;
};

// Generates VectorMakerStats for a given vector of nullable elements.
template <typename T>
VectorMakerStats<EvalType<T>> genVectorMakerStats(
    const std::vector<std::optional<T>>& data) {
  using TEvalType = EvalType<T>;
  VectorMakerStats<TEvalType> result;

  // Count distinct and null elements.
  for (const auto& val : data) {
    if (val == std::nullopt) {
      ++result.nullCount;
    } else {
      result.addElement(static_cast<TEvalType>(*val));
    }
  }

  // Sorted state.
  result.isSorted = std::is_sorted(data.begin(), data.end());

  // Calculate min and max (skip null elements).
  for (const auto& val : data) {
    if (val != std::nullopt) {
      auto nativeVal = static_cast<TEvalType>(*val);
      result.min = (result.min == std::nullopt)
          ? nativeVal
          : std::min(*result.min, nativeVal);
      result.max = (result.max == std::nullopt)
          ? nativeVal
          : std::max(*result.max, nativeVal);
    }
  }
  return result;
}

// Generates VectorMakerStats for a given vector of non-nullable elements.
template <typename T>
VectorMakerStats<EvalType<T>> genVectorMakerStats(const std::vector<T>& data) {
  using TEvalType = EvalType<T>;
  VectorMakerStats<TEvalType> result;
  for (const auto& val : data) {
    result.addElement(static_cast<TEvalType>(val));
  }

  result.isSorted = std::is_sorted(data.begin(), data.end());
  const auto& [min, max] = std::minmax_element(data.begin(), data.end());
  if (min != data.end()) {
    result.min = static_cast<TEvalType>(*min);
  }
  if (max != data.end()) {
    result.max = static_cast<TEvalType>(*max);
  }
  return result;
}

} // namespace bytedance::bolt::test
