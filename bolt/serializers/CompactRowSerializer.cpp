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

#include "bolt/serializers/CompactRowSerializer.h"
#include "bolt/row/CompactRow.h"
#include "bolt/serializers/RowSerializer.h"

namespace bytedance::bolt::serializer {

void CompactRowVectorSerde::estimateSerializedSize(
    VectorPtr /* vector */,
    const folly::Range<const IndexRange*>& /* ranges */,
    vector_size_t** /* sizes */,
    Scratch& /*scratch*/) {
  BOLT_UNSUPPORTED();
}

namespace {
class CompactRowVectorSerializer : public RowSerializer<row::CompactRow> {
 public:
  explicit CompactRowVectorSerializer(memory::MemoryPool* pool)
      : RowSerializer<row::CompactRow>(pool) {}

 private:
  void serializeRanges(
      const row::CompactRow& row,
      const folly::Range<const IndexRange*>& ranges,
      char* rawBuffer,
      const std::vector<vector_size_t>& rowSize) override {
    size_t offset = 0;
    vector_size_t index = 0;
    for (const auto& range : ranges) {
      if (range.size == 1) {
        // Fast path for single-row serialization.
        *(TRowSize*)(rawBuffer + offset) = folly::Endian::big(rowSize[index]);
        auto size =
            row.serialize(range.begin, rawBuffer + offset + sizeof(TRowSize));
        offset += size + sizeof(TRowSize);
        ++index;
      } else {
        raw_vector<size_t> offsets(range.size);
        for (auto i = 0; i < range.size; ++i, ++index) {
          // Write raw size. Needs to be in big endian order.
          *(TRowSize*)(rawBuffer + offset) = folly::Endian::big(rowSize[index]);
          offsets[i] = offset + sizeof(TRowSize);
          offset += rowSize[index] + sizeof(TRowSize);
        }
        // Write row data for all rows in range.
        row.serialize(range.begin, range.size, offsets.data(), rawBuffer);
      }
    }
  }
};

} // namespace

std::unique_ptr<VectorSerializer> CompactRowVectorSerde::createSerializer(
    RowTypePtr /* type */,
    int32_t /* numRows */,
    StreamArena* streamArena,
    const Options* /* options */) {
  return std::make_unique<CompactRowVectorSerializer>(streamArena->pool());
}

void CompactRowVectorSerde::deserialize(
    ByteInputStream* source,
    bolt::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    const Options* /* options */) {
  std::vector<std::string_view> serializedRows;
  std::vector<std::string> serializedBuffers;

  RowDeserializer<std::string_view>::deserialize(
      source, serializedRows, serializedBuffers);

  if (serializedRows.empty()) {
    *result = BaseVector::create<RowVector>(type, 0, pool);
    return;
  }

  *result = bolt::row::CompactRow::deserialize(serializedRows, type, pool);
}

// static
void CompactRowVectorSerde::registerVectorSerde() {
  bolt::registerVectorSerde(std::make_unique<CompactRowVectorSerde>());
}

// static
void CompactRowVectorSerde::registerNamedVectorSerde() {
  bolt::registerNamedVectorSerde(
      VectorSerde::Kind::kCompactRow,
      std::make_unique<CompactRowVectorSerde>());
}

} // namespace bytedance::bolt::serializer
