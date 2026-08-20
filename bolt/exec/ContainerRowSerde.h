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

#include "bolt/common/memory/ByteStream.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/DecodedVector.h"
namespace bytedance::bolt::exec {

struct ContainerRowSerdeOptions {
  /// Used for ex., sorting maps by map keys, when occurring as key.
  bool isKey = true;
};

/// Row-wise serialization for use in hash tables and order by.
class ContainerRowSerde {
 public:
  /// Serializes value from source[index] into 'out'. The value must not be
  /// null.
  static void serialize(
      const BaseVector& source,
      vector_size_t index,
      ByteOutputStream& out,
      const ContainerRowSerdeOptions& options);

  static void deserialize(
      ByteInputStream& in,
      vector_size_t index,
      BaseVector* result,
      bool exactSize = false);

  /// Returns < 0 if 'left' is less than 'right' at 'index', 0 if
  /// equal and > 0 otherwise. flags.nullHandlingMode can be only NullAsValue
  /// and support null-safe equal. Top level rows in right are not allowed to be
  /// null. Note that the assumption for Map is the serialized map entries are
  /// sorted in the order of key to be comparable.
  static int32_t compare(
      ByteInputStream& left,
      const DecodedVector& right,
      vector_size_t index,
      CompareFlags flags);

  /// Returns < 0 if 'left' is less than 'right' at 'index', 0 if
  /// equal and > 0 otherwise. flags.nullHandlingMode can be only NullAsValue
  /// and support null-safe equal. Note that the assumption for Map is the
  /// serialized map entries are sorted in the order of key to be comparable.
  static int32_t compare(
      ByteInputStream& left,
      ByteInputStream& right,
      const Type* type,
      CompareFlags flags);

  /// Returns < 0 if 'left' is less than 'right' at 'index', 0 if
  /// equal and > 0 otherwise. If flags.nullHandlingMode is StopAtNull,
  /// returns std::nullopt if either 'left' or 'right' value is null or contains
  /// a null. If flags.nullHandlingMode is NullAsValue then NULL is considered
  /// equal to NULL. Top level rows in right are not allowed to be null. Note
  /// that the assumption for Map is the serialized map entries are
  /// sorted in the order of key to be comparable.
  static std::optional<int32_t> compareWithNulls(
      ByteInputStream& left,
      const DecodedVector& right,
      vector_size_t index,
      CompareFlags flags);

  /// Returns < 0 if 'left' is less than 'right' at 'index', 0 if
  /// equal and > 0 otherwise. If flags.nullHandlingMode is StopAtNull,
  /// returns std::nullopt if either 'left' or 'right' value is null or contains
  /// a null. If flags.nullHandlingMode is NullAsValue then NULL is considered
  /// equal to NULL. Note that the assumption for Map is the serialized map
  /// entries are sorted in the order of key to be comparable.
  static std::optional<int32_t> compareWithNulls(
      ByteInputStream& left,
      ByteInputStream& right,
      const Type* type,
      CompareFlags flags);

  static uint64_t hash(ByteInputStream& data, const Type* type);
};

} // namespace bytedance::bolt::exec
