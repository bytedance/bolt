/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include "PrestoSerializer.h"
#include "bolt/common/base/Crc.h"
#include "bolt/common/compression/Compression.h"
#include "bolt/vector/VectorStream.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::serializer::arrowserde {

class ArrowVectorSerde : public VectorSerde {
 public:
  struct ArrowSerdeOptions : VectorSerde::Options {
    ArrowSerdeOptions() : arrowOptions(defaultArrowOptions()) {}

    ArrowSerdeOptions(
        bool _useLosslessTimestamp,
        common::CompressionKind _compressionKind)
        : Options(_useLosslessTimestamp, _compressionKind),
          useLosslessTimestamp(_useLosslessTimestamp),
          compressionKind(_compressionKind),
          arrowOptions(defaultArrowOptions(_useLosslessTimestamp)) {}

    bool useLosslessTimestamp{false};
    common::CompressionKind compressionKind{
        common::CompressionKind::CompressionKind_NONE};
    std::vector<VectorEncoding::Simple> encodings;

    ArrowOptions arrowOptions;

    static ArrowSerdeOptions fromPrestoOptions(
        presto::PrestoVectorSerde::PrestoOptions* prestoOptions);

   private:
    static ArrowOptions defaultArrowOptions(bool losslessTs = false) {
      ArrowOptions arrowOptions;
      arrowOptions.timestampUnit =
          losslessTs ? TimestampUnit::kNano : TimestampUnit::kMicro;
      arrowOptions.flattenDictionary = true;
      arrowOptions.flattenConstant = true;
      arrowOptions.exportToView = true;
      arrowOptions.stringViewCopyValues = true;
      arrowOptions.exportToArrowIPC = true;
      return arrowOptions;
    }
  };

  ArrowVectorSerde() : VectorSerde(Kind::kArrow) {}

  void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes,
      Scratch& scratch) override;

  void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const vector_size_t*> rows,
      vector_size_t** sizes,
      Scratch& scratch) override;

  std::unique_ptr<VectorSerializer> createSerializer(
      RowTypePtr type,
      int32_t numRows,
      StreamArena* streamArena,
      const Options* options) override;

  std::unique_ptr<BatchVectorSerializer> createBatchSerializer(
      memory::MemoryPool* pool,
      const Options* options) override;

  [[deprecated("Use BatchVectorSerializer instead.")]] void
  deprecatedSerializeEncoded(
      const RowVectorPtr& vector,
      StreamArena* streamArena,
      const Options* options,
      OutputStream* out);

  bool supportsAppendInDeserialize() const override {
    return true;
  }

  void deserialize(
      ByteInputStream* source,
      bolt::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      const Options* options) override {
    return deserialize(source, pool, type, result, 0, options);
  }

  void deserialize(
      ByteInputStream* source,
      bolt::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      vector_size_t resultOffset,
      const Options* options) override;

  static void registerVectorSerde();
  static void registerNamedVectorSerde();
};

// Testing function for nested encodings. See comments in scatterStructNulls().
void testingScatterStructNulls(
    vector_size_t size,
    vector_size_t scatterSize,
    const vector_size_t* scatter,
    const uint64_t* incomingNulls,
    RowVector& row,
    vector_size_t rowOffset);

class ArrowOutputStreamListener : public OutputStreamListener {
 public:
  void onWrite(const char* s, std::streamsize count) override {
    if (!paused_) {
      crc_.process_bytes(s, static_cast<size_t>(count));
    }
  }

  void pause() {
    paused_ = true;
  }
  void resume() {
    paused_ = false;
  }
  auto crc() const {
    return crc_;
  }
  void reset() {
    crc_.reset();
  }

 private:
  bool paused_{false};
  bits::Crc32 crc_;
};
} // namespace bytedance::bolt::serializer::arrowserde
