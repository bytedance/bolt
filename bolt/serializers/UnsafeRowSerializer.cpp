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

#include "bolt/serializers/UnsafeRowSerializer.h"
#include <folly/lang/Bits.h>
#include "bolt/row/UnsafeRowDeserializers.h"
#include "bolt/row/UnsafeRowFast.h"
#include "bolt/serializers/RowSerializer.h"

namespace bytedance::bolt::serializer::spark {

void UnsafeRowVectorSerde::estimateSerializedSize(
    VectorPtr /* vector */,
    const folly::Range<const IndexRange*>& /* ranges */,
    vector_size_t** /* sizes */,
    Scratch& /*scratch*/) {
  BOLT_UNSUPPORTED();
}

std::unique_ptr<VectorSerializer> UnsafeRowVectorSerde::createSerializer(
    RowTypePtr /* type */,
    int32_t /* numRows */,
    StreamArena* streamArena,
    const Options* /* options */) {
  return std::make_unique<RowSerializer<row::UnsafeRowFast>>(
      streamArena->pool());
}

void UnsafeRowVectorSerde::deserialize(
    ByteInputStream* source,
    bolt::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    const Options* /* options */) {
  std::vector<std::optional<std::string_view>> serializedRows;
  std::vector<std::string> serializedBuffers;

  RowDeserializer<std::optional<std::string_view>>::deserialize(
      source, serializedRows, serializedBuffers);

  if (serializedRows.empty()) {
    *result = BaseVector::create<RowVector>(type, 0, pool);
    return;
  }

  *result = std::dynamic_pointer_cast<RowVector>(
      bolt::row::UnsafeRowDeserializer::deserialize(
          serializedRows, type, pool));
}

// static
void UnsafeRowVectorSerde::registerVectorSerde() {
  bolt::registerVectorSerde(std::make_unique<UnsafeRowVectorSerde>());
}

// static
void UnsafeRowVectorSerde::registerNamedVectorSerde() {
  bolt::registerNamedVectorSerde(
      VectorSerde::Kind::kUnsafeRow, std::make_unique<UnsafeRowVectorSerde>());
}

} // namespace bytedance::bolt::serializer::spark
