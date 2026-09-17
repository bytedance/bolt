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

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/DecodedVector.h"

#include <span>

// Direct (span-based) serialization built on the ContainerRowSerde byte format,
// used by the BmRowContainer store paths. Unlike ContainerRowSerde's
// ByteOutputStream API, these entry points compute exact serialized sizes and
// write into caller-provided memory ranges. They reuse ContainerRowSerdeOptions
// and produce output identical to ContainerRowSerde::serialize().
namespace bytedance::bolt::exec {

/// Span-based serialization for BmRowContainer. The byte format matches
/// ContainerRowSerde::serialize(); MAP keys are serialized in canonical
/// (sorted) order when options.isKey is set.
class BmContainerRowSerde {
 public:
  /// Returns the exact number of bytes written by
  /// ContainerRowSerde::serialize() for a non-null top-level value.
  static uint64_t serializedSize(
      const BaseVector& source,
      vector_size_t index,
      const ContainerRowSerdeOptions& options);

  /// Computes exact serialized sizes for source rows [offset, offset + size).
  /// The result span must contain exactly 'size' entries. Null top-level
  /// values have size zero.
  static void serializedSizes(
      const DecodedVector& decoded,
      vector_size_t offset,
      vector_size_t size,
      const ContainerRowSerdeOptions& options,
      std::span<uint64_t> result);

  /// Serializes a non-null top-level value into an exact-size memory range.
  /// Throws if target is either too small or too large.
  static uint64_t serializeInto(
      const BaseVector& source,
      vector_size_t index,
      std::span<char> target,
      const ContainerRowSerdeOptions& options);
};

// Internal, non-public serialization helpers shared between BmContainerRowSerde
// and its in-repo callers (BmRowContainer store paths, targeted tests). These
// are deliberately kept out of the public ContainerRowSerde.h.
namespace detail {

/// Result of trySerializeInto(). 'size' is always the exact serialized byte
/// length of the value, even when it did not fit. 'complete' is true only when
/// the whole value was copied into the provided buffer.
struct TrySerializeResult {
  uint64_t size{0};
  bool complete{false};
};

/// Best-effort serialization of a non-null top-level value into 'available',
/// counting bytes for the entire traversal. While the running cursor is still
/// inside 'available' the bytes are copied; once it would overflow, copying
/// stops but the traversal continues so 'size' is exact. Capacity exhaustion is
/// reported via TrySerializeResult::complete == false, never thrown. Genuine
/// serde errors (unsupported type, per-field int32 length overflow, ...) still
/// throw as in serialize()/serializeInto().
///
/// The byte format is identical to ContainerRowSerde::serialize(); MAP keys are
/// serialized in canonical (sorted) order when options.isKey is set.
TrySerializeResult trySerializeInto(
    const BaseVector& source,
    vector_size_t index,
    std::span<char> available,
    const ContainerRowSerdeOptions& options);

} // namespace detail

} // namespace bytedance::bolt::exec
