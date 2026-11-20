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

#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace bytedance::bolt::memory::bm {

enum class BlockSchemaKind : uint8_t {
  kOpaque,
  kFixedRow,
};

enum class BlockFieldKind : uint8_t {
  kSignedInteger,
  kUnsignedInteger,
  kFloatingPoint,
  kOpaque,
};

struct BlockFieldSchema {
  BlockFieldKind kind;
  uint32_t offset;
  uint32_t width;

  bool operator==(const BlockFieldSchema&) const = default;
};

struct FixedRowBlockSchema {
  uint32_t rowStride;
  std::vector<BlockFieldSchema> fields;
};

struct OpaqueBlockSchema {};

struct BlockDescriptor {
  BlockSchemaKind schemaKind;
  uint32_t elementCount;
  std::variant<OpaqueBlockSchema, FixedRowBlockSchema> schema;
};

/// Returns fields ordered by offset with every uncovered byte represented as
/// an opaque field. Adjacent opaque spans are coalesced.
std::vector<BlockFieldSchema> NormalizeFixedRowFields(
    const FixedRowBlockSchema& schema);

void ValidateBlockDescriptor(
    const BlockDescriptor& descriptor,
    size_t blockSize);

} // namespace bytedance::bolt::memory::bm
