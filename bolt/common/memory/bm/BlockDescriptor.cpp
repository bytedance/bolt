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

#include "bolt/common/memory/bm/BlockDescriptor.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <limits>

namespace bytedance::bolt::memory::bm {
namespace {

void validateFieldKind(BlockFieldKind kind) {
  switch (kind) {
    case BlockFieldKind::kSignedInteger:
    case BlockFieldKind::kUnsignedInteger:
    case BlockFieldKind::kFloatingPoint:
    case BlockFieldKind::kOpaque:
      return;
  }
  BOLT_FAIL("BM block descriptor contains an unknown field kind");
}

void appendNormalizedField(
    std::vector<BlockFieldSchema>& fields,
    BlockFieldSchema field) {
  if (!fields.empty() && field.kind == BlockFieldKind::kOpaque &&
      fields.back().kind == BlockFieldKind::kOpaque &&
      fields.back().offset + fields.back().width == field.offset) {
    fields.back().width += field.width;
    return;
  }
  fields.push_back(field);
}

} // namespace

std::vector<BlockFieldSchema> NormalizeFixedRowFields(
    const FixedRowBlockSchema& schema) {
  BOLT_CHECK_GT(schema.rowStride, 0, "BM fixed-row stride must be positive");

  auto ordered = schema.fields;
  std::sort(
      ordered.begin(), ordered.end(), [](const auto& left, const auto& right) {
        if (left.offset != right.offset) {
          return left.offset < right.offset;
        }
        return left.width < right.width;
      });

  std::vector<BlockFieldSchema> normalized;
  normalized.reserve(ordered.size() * 2 + 1);
  uint32_t cursor = 0;
  for (const auto& field : ordered) {
    validateFieldKind(field.kind);
    BOLT_CHECK_GT(field.width, 0, "BM block field width must be positive");
    BOLT_CHECK_LE(
        field.offset,
        schema.rowStride,
        "BM block field offset exceeds row stride");
    BOLT_CHECK_LE(
        field.width,
        schema.rowStride - field.offset,
        "BM block field exceeds row stride");
    BOLT_CHECK_GE(
        field.offset, cursor, "BM block descriptor fields must not overlap");
    if (field.offset > cursor) {
      appendNormalizedField(
          normalized,
          BlockFieldSchema{
              BlockFieldKind::kOpaque, cursor, field.offset - cursor});
    }
    appendNormalizedField(normalized, field);
    cursor = field.offset + field.width;
  }
  if (cursor < schema.rowStride) {
    appendNormalizedField(
        normalized,
        BlockFieldSchema{
            BlockFieldKind::kOpaque, cursor, schema.rowStride - cursor});
  }
  return normalized;
}

void ValidateBlockDescriptor(
    const BlockDescriptor& descriptor,
    size_t blockSize) {
  BOLT_CHECK_GT(blockSize, 0, "BM block size must be positive");
  switch (descriptor.schemaKind) {
    case BlockSchemaKind::kOpaque:
      BOLT_CHECK(
          std::holds_alternative<OpaqueBlockSchema>(descriptor.schema),
          "BM opaque descriptor has mismatched schema payload");
      return;
    case BlockSchemaKind::kFixedRow: {
      BOLT_CHECK(
          std::holds_alternative<FixedRowBlockSchema>(descriptor.schema),
          "BM fixed-row descriptor has mismatched schema payload");
      const auto& schema = std::get<FixedRowBlockSchema>(descriptor.schema);
      NormalizeFixedRowFields(schema);
      constexpr auto kMaxSize = std::numeric_limits<size_t>::max();
      BOLT_CHECK(
          descriptor.elementCount == 0 ||
              schema.rowStride <= kMaxSize / descriptor.elementCount,
          "BM fixed-row structured prefix size overflows size_t");
      const auto structuredBytes =
          static_cast<size_t>(descriptor.elementCount) * schema.rowStride;
      BOLT_CHECK_LE(
          structuredBytes,
          blockSize,
          "BM fixed-row structured prefix exceeds block size");
      return;
    }
  }
  BOLT_FAIL("BM block descriptor contains an unknown schema kind");
}

} // namespace bytedance::bolt::memory::bm
