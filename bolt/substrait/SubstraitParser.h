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

#include "bolt/substrait/proto/substrait/algebra.pb.h"
#include "bolt/substrait/proto/substrait/capabilities.pb.h"
#include "bolt/substrait/proto/substrait/extensions/extensions.pb.h"
#include "bolt/substrait/proto/substrait/function.pb.h"
#include "bolt/substrait/proto/substrait/parameterized_types.pb.h"
#include "bolt/substrait/proto/substrait/plan.pb.h"
#include "bolt/substrait/proto/substrait/type.pb.h"
#include "bolt/substrait/proto/substrait/type_expressions.pb.h"

#include <memory>
#include <unordered_map>
#include "bolt/type/Type.h"
namespace bytedance::bolt::substrait {

/// This class contains some common functions used to parse Substrait
/// components, and convert them into recognizable representations.
class SubstraitParser {
 public:
  /// Parse Substrait NamedStruct.
  std::vector<TypePtr> parseNamedStruct(
      const ::substrait::NamedStruct& namedStruct);

  /// Parse Substrait Type.
  TypePtr parseType(const ::substrait::Type& substraitType);

  /// Parse Substrait ReferenceSegment.
  int32_t parseReferenceSegment(
      const ::substrait::Expression::ReferenceSegment& refSegment);

  /// Make names in the format of {prefix}_{index}.
  std::vector<std::string> makeNames(const std::string& prefix, int size);

  /// Make node name in the format of n{nodeId}_{colIdx}.
  std::string makeNodeName(int nodeId, int colIdx);

  /// Get the column index from a node name in the format of
  /// n{nodeId}_{colIdx}.
  int getIdxFromNodeName(const std::string& nodeName);

  /// Find the Substrait function name according to the function id
  /// from a pre-constructed function map. The function specification can be
  /// a simple name or a compound name. The compound name format is:
  /// <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>.
  /// Currently, the input types in the function specification are not used. But
  /// in the future, they should be used for the validation according the
  /// specifications in Substrait yaml files.
  static const std::string& findFunctionSpec(
      const std::unordered_map<uint64_t, std::string>& functionMap,
      uint64_t id);

  /// Find the Substrait function specification using the internal function map
  /// set via setFunctionMap.
  const std::string& findFunctionSpec(uint64_t id) const;

  /// This function is used get the types from the compound name.
  static std::vector<std::string> getSubFunctionTypes(
      const std::string& substraitFunction);

  /// Find the Bolt function name according to the function id
  /// from a pre-constructed function map.
  std::string findBoltFunction(
      const std::unordered_map<uint64_t, std::string>& functionMap,
      uint64_t id) const;

  /// Find the Bolt function name using the internal function map set via
  /// setFunctionMap.
  std::string findBoltFunction(uint64_t id) const;

  /// Map the Substrait function keyword into Bolt function keyword.
  std::string mapToBoltFunction(const std::string& substraitFunction) const;

  /// Get input types from Substrait function signature.
  static std::vector<TypePtr> getInputTypes(const std::string& signature);

 private:
  /// Mapping from Substrait type anchors (references) to extension names.
  /// Populated from Plan.extensions by the plan converter.
  std::unordered_map<uint32_t, std::string> typeExtensions_{{0, "UNKNOWN"}};

  /// Mapping from function anchors to function specifications set by plan
  /// converter.
  std::unordered_map<uint64_t, std::string> functionMap_;

 public:
  /// Set the map from type anchors to extension names.
  void setTypeExtensionMap(
      const std::unordered_map<uint32_t, std::string>& typeExtensions) {
    typeExtensions_ = typeExtensions;
  }

  /// Set the map from function anchors to function specifications.
  void setFunctionMap(
      const std::unordered_map<uint64_t, std::string>& functionMap) {
    functionMap_ = functionMap;
  }

  /// Resolve a type extension anchor to its declared name.
  /// Throws if the anchor is unknown.
  const std::string& getTypeExtensionName(uint32_t anchor) const;

 private:
  /// A map used for mapping Substrait function keywords into Bolt functions'
  /// keywords. Key: the Substrait function keyword, Value: the Bolt function
  /// keyword. For those functions with different names in Substrait and Bolt,
  /// a mapping relation should be added here.
  std::unordered_map<std::string, std::string> substraitBoltFunctionMap_ = {
      {"add", "plus"},
      {"subtract", "minus"},
      {"modulus", "mod"},
      {"not_equal", "neq"},
      {"equal", "eq"},
      // Normalize negated null-check into built-in is_not_null
      {"NOT is_null", "is_not_null"}};
};

} // namespace bytedance::bolt::substrait
