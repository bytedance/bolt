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

#include <boost/algorithm/string.hpp>

#include "bolt/common/base/Exceptions.h"
#include "bolt/expression/TypeSignature.h"
namespace bytedance::bolt::exec {
namespace {
std::vector<TypeSignature> MakeParameters(const Type& type) {
  std::vector<TypeSignature> parameters;
  switch (type.kind()) {
    case TypeKind::ROW: {
      const auto& row_type = dynamic_cast<const RowType&>(type);
      for (size_t i = 0; i < row_type.children().size(); i++) {
        parameters.emplace_back(*row_type.children()[i], row_type.names()[i]);
      }
      break;
    }
    case TypeKind::ARRAY: {
      const auto& array_type = dynamic_cast<const ArrayType&>(type);
      for (size_t i = 0; i < array_type.children().size(); i++) {
        parameters.emplace_back(*array_type.children()[i]);
      }
      break;
    }
    case TypeKind::MAP: {
      const auto& map_type = dynamic_cast<const MapType&>(type);
      for (size_t i = 0; i < map_type.children().size(); i++) {
        parameters.emplace_back(*map_type.children()[i], map_type.names()[i]);
      }
      break;
    }
    case TypeKind::FUNCTION: {
      const auto& function_type = dynamic_cast<const FunctionType&>(type);
      for (size_t i = 0; i < function_type.children().size(); i++) {
        parameters.emplace_back(*function_type.children()[i]);
      }
      break;
    }
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
    case TypeKind::UNKNOWN:
    case TypeKind::VARIANT:
      break;
      // Possibly a decimal type with scale and precision arguments,
    case TypeKind::BIGINT: {
      if (auto* decimal_type =
              dynamic_cast<const DecimalType<TypeKind::BIGINT>*>(&type)) {
        for (const auto& p : decimal_type->parameters()) {
          switch (p.kind) {
            case TypeParameterKind::kLongLiteral:
              parameters.emplace_back(fmt::format("{}", p.longLiteral.value()));
              break;
            case TypeParameterKind::kType:
              parameters.emplace_back(*p.type);
              break;
          }
        }
      }
      break;
    }
    case TypeKind::HUGEINT: {
      if (auto* decimal_type =
              dynamic_cast<const DecimalType<TypeKind::HUGEINT>*>(&type)) {
        for (const auto& p : decimal_type->parameters()) {
          switch (p.kind) {
            case TypeParameterKind::kLongLiteral:
              parameters.emplace_back(fmt::format("{}", p.longLiteral.value()));
              break;
            case TypeParameterKind::kType:
              parameters.emplace_back(*p.type);
              break;
          }
        }
      }
      break;
    }
    case TypeKind::OPAQUE:
    case TypeKind::INVALID:
      BOLT_FAIL(
          "Unsupported type to signature conversion: {}", type.toString());
  }

  return parameters;
}
} // namespace

void toAppend(
    const bytedance::bolt::exec::TypeSignature& signature,
    std::string* result) {
  result->append(signature.toString());
}

std::string TypeSignature::toString() const {
  std::ostringstream out;
  if (rowFieldName_.has_value()) {
    out << *rowFieldName_ << " ";
  }
  out << baseName_;
  if (!parameters_.empty()) {
    out << "(" << folly::join(",", parameters_) << ")";
  }
  return out.str();
}

TypePtr TypeSignature::asType() const {
  auto basename = boost::algorithm::to_upper_copy(baseName_);
  std::vector<TypeParameter> parameters;
  std::vector<std::string> rowParameterNames;

  parameters.reserve(parameters_.size());
  rowParameterNames.reserve(parameters_.size());

  for (const auto& p : parameters_) {
    rowParameterNames.emplace_back(
        p.rowFieldName_.has_value() ? p.rowFieldName_.value() : std::string());
    if (isDecimalName(basename)) {
      parameters.emplace_back(std::stoll(p.baseName_));
    } else {
      parameters.emplace_back(p.asType());
    }
  }

  return getType(basename, parameters, rowParameterNames);
}

TypeSignature::TypeSignature(
    const Type& type,
    std::optional<std::string> rowFieldName)
    : baseName_(type.name()),
      parameters_(MakeParameters(type)),
      rowFieldName_(std::move(rowFieldName)) {}

} // namespace bytedance::bolt::exec
