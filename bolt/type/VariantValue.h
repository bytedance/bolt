/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <string>
#include "bolt/type/StringView.h"

namespace bytedance::bolt {

struct VariantValue {
  StringView value;
  StringView metadata;

  bool operator==(const VariantValue& other) const {
    return value == other.value && metadata == other.metadata;
  }

  bool operator!=(const VariantValue& other) const {
    return !(*this == other);
  }

  bool operator<(const VariantValue& other) const {
    if (value != other.value) {
      return value < other.value;
    }
    return metadata < other.metadata;
  }

  void copy_from(const VariantValue& other) {
    value = other.value;
    metadata = other.metadata;
  }

  std::string toString() const {
    return "Variant(" + std::to_string(value.size()) + " bytes, " +
        std::to_string(metadata.size()) + " bytes)";
  }

  int32_t compare(const VariantValue& other) const {
    int32_t res = value.compare(other.value);
    if (res != 0) {
      return res;
    }
    return metadata.compare(other.metadata);
  }
};

struct OwnedVariantValue {
  std::string value;
  std::string metadata;

  OwnedVariantValue() = default;
  OwnedVariantValue(const VariantValue& v)
      : value(v.value.data(), v.value.size()),
        metadata(v.metadata.data(), v.metadata.size()) {}
  OwnedVariantValue(std::string v, std::string m)
      : value(std::move(v)), metadata(std::move(m)) {}

  bool operator==(const OwnedVariantValue& other) const {
    return value == other.value && metadata == other.metadata;
  }

  bool operator<(const OwnedVariantValue& other) const {
    if (value != other.value) {
      return value < other.value;
    }
    return metadata < other.metadata;
  }

  operator VariantValue() const {
    return {StringView(value), StringView(metadata)};
  }

  std::string toString() const {
    return "Variant(" + std::to_string(value.size()) + " bytes, " +
        std::to_string(metadata.size()) + " bytes)";
  }
};

template <typename T>
void toAppend(const VariantValue& value, T* result) {
  result->append(value.toString());
}

template <typename T>
void toAppend(const OwnedVariantValue& value, T* result) {
  result->append(value.toString());
}

} // namespace bytedance::bolt

namespace folly {
template <>
struct hasher<::bytedance::bolt::VariantValue> {
  size_t operator()(const ::bytedance::bolt::VariantValue& value) const {
    return ::bytedance::bolt::bits::hashMix(
        hasher<::bytedance::bolt::StringView>{}(value.value),
        hasher<::bytedance::bolt::StringView>{}(value.metadata));
  }
};

template <>
struct hasher<::bytedance::bolt::OwnedVariantValue> {
  size_t operator()(const ::bytedance::bolt::OwnedVariantValue& value) const {
    return hasher<::bytedance::bolt::VariantValue>{}(value);
  }
};
} // namespace folly

namespace std {
template <>
struct hash<::bytedance::bolt::VariantValue> {
  size_t operator()(const ::bytedance::bolt::VariantValue& value) const {
    return ::folly::hasher<::bytedance::bolt::VariantValue>{}(value);
  }
};

template <>
struct hash<::bytedance::bolt::OwnedVariantValue> {
  size_t operator()(const ::bytedance::bolt::OwnedVariantValue& value) const {
    return ::folly::hasher<::bytedance::bolt::OwnedVariantValue>{}(value);
  }
};
} // namespace std
