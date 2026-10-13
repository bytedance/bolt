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

#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"

#include <fmt/format.h>
#include <folly/dynamic.h>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {
namespace {

template <TypeKind kind>
class LanceSemanticScalarType final : public ScalarType<kind> {
 public:
  explicit LanceSemanticScalarType(std::string semantic)
      : semantic_(std::move(semantic)), name_("LANCE_" + semantic_) {}

  const char* name() const override {
    return name_.c_str();
  }

  std::string toString() const override {
    return name_;
  }

  bool equivalent(const Type& other) const override {
    const auto* semantic =
        dynamic_cast<const LanceSemanticScalarType<kind>*>(&other);
    return semantic != nullptr && semantic->semantic_ == semantic_;
  }

  folly::dynamic serialize() const override {
    auto result = ScalarType<kind>::serialize();
    result["type"] = TypeTraits<kind>::name;
    result["lanceSemantic"] = semantic_;
    return result;
  }

  std::string_view semantic() const {
    return semantic_;
  }

 private:
  const std::string semantic_;
  const std::string name_;
};

template <TypeKind kind>
TypePtr semanticType(std::string semantic) {
  return std::make_shared<const LanceSemanticScalarType<kind>>(
      std::move(semantic));
}

class DefaultNativeLanceTypeAdapter final : public NativeLanceTypeAdapter {
 public:
  std::optional<TypePtr> resolve(const Request& request) const override {
    const auto requireKind = [&](TypeKind kind) {
      BOLT_CHECK_EQ(
          request.storageType->kind(),
          kind,
          "Lance semantic type '{}' has incompatible storage type {}",
          request.logicalType,
          request.storageType->toString());
    };
    if (request.logicalType.rfind("time32:", 0) == 0 ||
        request.logicalType.rfind("time64:", 0) == 0) {
      requireKind(TypeKind::BIGINT);
      return semanticType<TypeKind::BIGINT>(std::string(request.logicalType));
    }
    if (request.logicalType.rfind("timestamp:", 0) == 0) {
      requireKind(TypeKind::TIMESTAMP);
      const auto timezone = request.logicalType.rfind(':');
      BOLT_CHECK_NE(
          timezone,
          std::string_view::npos,
          "Invalid Lance timestamp logical type '{}'",
          request.logicalType);
      if (request.logicalType.substr(timezone + 1) != "-") {
        return semanticType<TypeKind::TIMESTAMP>(
            std::string(request.logicalType));
      }
      return std::nullopt;
    }
    if (request.extensionName == "lance.json" ||
        request.extensionName == "arrow.json") {
      requireKind(TypeKind::VARBINARY);
      return semanticType<TypeKind::VARBINARY>(
          request.extensionMetadata.empty()
              ? fmt::format("json:{}", request.extensionName)
              : fmt::format(
                    "json:{}:{}",
                    request.extensionName,
                    request.extensionMetadata));
    }
    if (request.extensionName == "lance.bfloat16") {
      requireKind(TypeKind::VARBINARY);
      return semanticType<TypeKind::VARBINARY>("bfloat16");
    }
    if (request.logicalType == "lance.bfloat16") {
      requireKind(TypeKind::VARBINARY);
      return semanticType<TypeKind::VARBINARY>("bfloat16");
    }
    return std::nullopt;
  }
};

} // namespace

std::shared_ptr<const NativeLanceTypeAdapter> defaultNativeLanceTypeAdapter() {
  static const auto adapter =
      std::make_shared<const DefaultNativeLanceTypeAdapter>();
  return adapter;
}

std::optional<std::string_view> nativeLanceSemanticType(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BIGINT:
      if (const auto* semantic =
              dynamic_cast<const LanceSemanticScalarType<TypeKind::BIGINT>*>(
                  type.get())) {
        return semantic->semantic();
      }
      break;
    case TypeKind::TIMESTAMP:
      if (const auto* semantic =
              dynamic_cast<const LanceSemanticScalarType<TypeKind::TIMESTAMP>*>(
                  type.get())) {
        return semantic->semantic();
      }
      break;
    case TypeKind::VARBINARY:
      if (const auto* semantic =
              dynamic_cast<const LanceSemanticScalarType<TypeKind::VARBINARY>*>(
                  type.get())) {
        return semantic->semantic();
      }
      break;
    default:
      break;
  }
  return std::nullopt;
}

std::string_view nativeLanceDictionaryValueLogicalType(
    std::string_view logicalType) {
  constexpr auto kPrefix = std::string_view("dict:");
  BOLT_CHECK_EQ(
      logicalType.rfind(kPrefix, 0),
      0,
      "Invalid Lance dictionary logical type: {}",
      logicalType);
  const auto orderedSeparator = logicalType.rfind(':');
  BOLT_CHECK_NE(
      orderedSeparator,
      std::string_view::npos,
      "Invalid Lance dictionary logical type: {}",
      logicalType);
  const auto orderedToken = logicalType.substr(orderedSeparator + 1);
  BOLT_CHECK(
      orderedToken == "true" || orderedToken == "false",
      "Invalid Lance dictionary ordered flag: {}",
      logicalType);
  const auto indexSeparator = logicalType.rfind(':', orderedSeparator - 1);
  BOLT_CHECK_GT(
      indexSeparator,
      kPrefix.size(),
      "Invalid Lance dictionary logical type: {}",
      logicalType);
  const auto index = logicalType.substr(
      indexSeparator + 1, orderedSeparator - indexSeparator - 1);
  BOLT_CHECK(
      index == "int8" || index == "uint8" || index == "int16" ||
          index == "uint16" || index == "int32" || index == "uint32" ||
          index == "int64" || index == "uint64",
      "Invalid Lance dictionary index type: {}",
      logicalType);
  return logicalType.substr(kPrefix.size(), indexSeparator - kPrefix.size());
}

} // namespace bytedance::bolt::lance::reader
