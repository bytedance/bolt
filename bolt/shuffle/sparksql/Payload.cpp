/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include "bolt/shuffle/sparksql/Payload.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/util/bitmap.h>
#include <cstdint>
#include <iostream>
#include <numeric>

#include <functional>
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/Utils.h"

#include "bolt/common/base/SimdUtil.h"
namespace bytedance::bolt::shuffle::sparksql {
namespace {

static const Payload::Type kCompressedType = BlockPayload::kCompressed;
static const Payload::Type kUncompressedType = BlockPayload::kUncompressed;
static const Payload::Mode kBufferMode = BlockPayload::kBuffer;
static const Payload::Mode kRowVectorMode = BlockPayload::kRowVector;

static constexpr int64_t kZeroLengthBuffer = 0;
static constexpr int64_t kNullBuffer = -1;
static constexpr int64_t kUncompressedBuffer = -2;

using RowSizeType = int32_t;
static constexpr int32_t kRowSizeBytes = sizeof(RowSizeType);

template <typename T>
void write(uint8_t** dst, T data) {
  memcpy(*dst, &data, sizeof(T));
  *dst += sizeof(T);
}

template <typename T>
T* advance(uint8_t** dst) {
  auto ptr = reinterpret_cast<T*>(*dst);
  *dst += sizeof(T);
  return ptr;
}

FLATTEN arrow::Status internalReadFromCacheOrStream(
    arrow::io::InputStream* inputStream,
    ByteBuffer** readAheadBuffer,
    int64_t bytes,
    void* dst) {
  BOLT_DCHECK(*readAheadBuffer != nullptr);
  if ((*readAheadBuffer)->size >= bytes) {
    memcpy(dst, (*readAheadBuffer)->data, bytes);
    (*readAheadBuffer)->advance(bytes);
    if ((*readAheadBuffer)->size == 0) {
      *readAheadBuffer = nullptr;
    }
  } else {
    auto left = (*readAheadBuffer)->size;
    memcpy(dst, (*readAheadBuffer)->data, left);
    (*readAheadBuffer)->reset();
    *readAheadBuffer = nullptr;
    RETURN_NOT_OK(
        inputStream->Read(bytes - left, static_cast<uint8_t*>(dst) + left));
  }
  return arrow::Status::OK();
}

arrow::Result<std::tuple<uint8_t, uint32_t, uint8_t>> readTypeAndRows(
    arrow::io::InputStream* inputStream) {
  uint8_t type;
  uint32_t numRows;
  uint8_t mode;
  ARROW_ASSIGN_OR_RAISE(
      auto bytes, inputStream->Read(sizeof(Payload::Type), &type));
  if (bytes == 0) {
    // Reach EOS.
    return std::make_tuple(0, 0, 0);
  }
  RETURN_NOT_OK(inputStream->Read(sizeof(uint32_t), &numRows));
  RETURN_NOT_OK(inputStream->Read(sizeof(Payload::Mode), &mode));
  return std::make_tuple(type, numRows, mode);
}

arrow::Result<std::tuple<uint32_t, uint8_t>> readRowsAndMode(
    arrow::io::InputStream* inputStream,
    ByteBuffer** readAheadBuffer) {
  uint32_t numRows;
  uint8_t mode;
  if (*readAheadBuffer) {
    auto const len = sizeof(uint32_t) + sizeof(Payload::Mode);
    std::vector<uint8_t> tmp(5);
    uint8_t* data = tmp.data();
    RETURN_NOT_OK(
        internalReadFromCacheOrStream(inputStream, readAheadBuffer, len, data));
    numRows = *((uint32_t*)data);
    mode = *((uint8_t*)(data + sizeof(uint32_t)));
  } else {
    RETURN_NOT_OK(inputStream->Read(sizeof(uint32_t), &numRows));
    RETURN_NOT_OK(inputStream->Read(sizeof(Payload::Mode), &mode));
  }
  return std::make_tuple(numRows, mode);
}

arrow::Result<int64_t> compressBuffer(
    const std::shared_ptr<arrow::Buffer>& buffer,
    uint8_t* output,
    int64_t outputLength,
    arrow::util::Codec* codec) {
  auto outputPtr = &output;
  if (!buffer) {
    write<int64_t>(outputPtr, kNullBuffer);
    return sizeof(int64_t);
  }
  if (buffer->size() == 0) {
    write<int64_t>(outputPtr, kZeroLengthBuffer);
    return sizeof(int64_t);
  }
  static const int64_t kCompressedBufferHeaderLength = 2 * sizeof(int64_t);
  auto* compressedLengthPtr = advance<int64_t>(outputPtr);
  write(outputPtr, static_cast<int64_t>(buffer->size()));
  ARROW_ASSIGN_OR_RAISE(
      auto compressedLength,
      codec->Compress(
          buffer->size(), buffer->data(), outputLength, *outputPtr));
  if (compressedLength >= buffer->size()) {
    // Write uncompressed buffer.
    memcpy(*outputPtr, buffer->data(), buffer->size());
    *compressedLengthPtr = kUncompressedBuffer;
    return kCompressedBufferHeaderLength + buffer->size();
  }
  *compressedLengthPtr = static_cast<int64_t>(compressedLength);
  return kCompressedBufferHeaderLength + compressedLength;
}

arrow::Status compressAndFlush(
    const std::shared_ptr<arrow::Buffer>& buffer,
    arrow::io::OutputStream* outputStream,
    arrow::util::Codec* codec,
    arrow::MemoryPool* pool,
    uint64_t& compressTime,
    uint64_t& writeTime) {
  if (!buffer) {
    bytedance::bolt::NanosecondTimer timer(&writeTime);
    RETURN_NOT_OK(outputStream->Write(&kNullBuffer, sizeof(int64_t)));
    return arrow::Status::OK();
  }
  if (buffer->size() == 0) {
    bytedance::bolt::NanosecondTimer timer(&writeTime);
    RETURN_NOT_OK(outputStream->Write(&kZeroLengthBuffer, sizeof(int64_t)));
    return arrow::Status::OK();
  }
  std::unique_ptr<arrow::ResizableBuffer> compressed;
  int64_t compressedSize = 0;
  {
    bytedance::bolt::NanosecondTimer timer(&compressTime);
    auto maxCompressedLength =
        codec->MaxCompressedLen(buffer->size(), buffer->data());
    ARROW_ASSIGN_OR_RAISE(
        compressed,
        arrow::AllocateResizableBuffer(
            sizeof(int64_t) * 2 + maxCompressedLength, pool));
    auto output = compressed->mutable_data();
    ARROW_ASSIGN_OR_RAISE(
        compressedSize,
        compressBuffer(buffer, output, maxCompressedLength, codec));
  }

  {
    bytedance::bolt::NanosecondTimer timer(&writeTime);
    RETURN_NOT_OK(outputStream->Write(compressed->data(), compressedSize));
  }
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> readUncompressedBuffer(
    arrow::io::InputStream* inputStream,
    ByteBuffer** readAheadBuffer,
    arrow::MemoryPool* pool,
    const uint32_t paddedSize = 0) {
  int64_t bufferLength;
  if (*readAheadBuffer) {
    RETURN_NOT_OK(internalReadFromCacheOrStream(
        inputStream, readAheadBuffer, sizeof(int64_t), &bufferLength));
  } else {
    RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &bufferLength));
  }
  if (bufferLength == kNullBuffer) {
    return nullptr;
  }
  if (*readAheadBuffer) {
    ARROW_ASSIGN_OR_RAISE(
        auto buffer,
        arrow::AllocateResizableBuffer(bufferLength + paddedSize, pool));
    if (paddedSize > 0) {
      RETURN_NOT_OK(buffer->Resize(bufferLength, false));
    }
    RETURN_NOT_OK(internalReadFromCacheOrStream(
        inputStream,
        readAheadBuffer,
        bufferLength,
        const_cast<uint8_t*>(buffer->data())));
    return buffer;
  } else {
    ARROW_ASSIGN_OR_RAISE(
        auto buffer,
        arrow::AllocateResizableBuffer(bufferLength + paddedSize, pool));
    if (paddedSize > 0) {
      RETURN_NOT_OK(buffer->Resize(bufferLength, false));
    }
    RETURN_NOT_OK(
        inputStream->Read(bufferLength, const_cast<uint8_t*>(buffer->data())));
    return buffer;
  }
}

arrow::Result<std::shared_ptr<arrow::Buffer>> readCompressedBuffer(
    arrow::io::InputStream* inputStream,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::MemoryPool* pool,
    uint64_t& decompressTime,
    ByteBuffer** readAheadBuffer,
    const uint32_t paddedSize = 0) { // align to bytedance::bolt::simd::kPadding
  int64_t compressedLength;
  if (*readAheadBuffer) {
    RETURN_NOT_OK(internalReadFromCacheOrStream(
        inputStream, readAheadBuffer, sizeof(int64_t), &compressedLength));
  } else {
    RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &compressedLength));
  }
  if (compressedLength == kNullBuffer) {
    return nullptr;
  }
  if (compressedLength == kZeroLengthBuffer) {
    return zeroLengthNullBuffer();
  }

  int64_t uncompressedLength;
  if (*readAheadBuffer) {
    RETURN_NOT_OK(internalReadFromCacheOrStream(
        inputStream, readAheadBuffer, sizeof(int64_t), &uncompressedLength));
  } else {
    RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &uncompressedLength));
  }
  if (compressedLength == kUncompressedBuffer) {
    ARROW_ASSIGN_OR_RAISE(
        auto uncompressed,
        arrow::AllocateResizableBuffer(uncompressedLength + paddedSize, pool));
    if (paddedSize > 0) {
      RETURN_NOT_OK(uncompressed->Resize(uncompressedLength, false));
    }
    if (*readAheadBuffer) {
      RETURN_NOT_OK(internalReadFromCacheOrStream(
          inputStream,
          readAheadBuffer,
          uncompressedLength,
          const_cast<uint8_t*>(uncompressed->data())));
    } else {
      RETURN_NOT_OK(inputStream->Read(
          uncompressedLength, const_cast<uint8_t*>(uncompressed->data())));
    }
    return uncompressed;
  }
  ARROW_ASSIGN_OR_RAISE(
      auto compressed, arrow::AllocateBuffer(compressedLength, pool));
  if (*readAheadBuffer) {
    RETURN_NOT_OK(internalReadFromCacheOrStream(
        inputStream,
        readAheadBuffer,
        compressedLength,
        const_cast<uint8_t*>(compressed->data())));
  } else {
    RETURN_NOT_OK(inputStream->Read(
        compressedLength, const_cast<uint8_t*>(compressed->data())));
  }

  bytedance::bolt::NanosecondTimer timer(&decompressTime);
  ARROW_ASSIGN_OR_RAISE(
      auto output,
      arrow::AllocateResizableBuffer(uncompressedLength + paddedSize, pool));
  if (paddedSize > 0) {
    RETURN_NOT_OK(output->Resize(uncompressedLength, false));
  }
  RETURN_NOT_OK(codec->Decompress(
      compressedLength,
      compressed->data(),
      uncompressedLength,
      const_cast<uint8_t*>(output->data())));
  return output;
}

} // namespace

Payload::Payload(
    Payload::Type type,
    uint32_t numRows,
    const std::vector<bool>* isValidityBuffer,
    Payload::Mode mode)
    : type_(type),
      numRows_(numRows),
      isValidityBuffer_(isValidityBuffer),
      mode_(mode) {}

std::string Payload::toString() const {
  static std::string kUncompressedString = "Payload::kUncompressed";
  static std::string kCompressedString = "Payload::kCompressed";
  static std::string kToBeCompressedString = "Payload::kToBeCompressed";

  if (type_ == kUncompressed) {
    return kUncompressedString;
  }
  if (type_ == kCompressed) {
    return kCompressedString;
  }
  return kToBeCompressedString;
}

void BlockPayload::concatBuffer(
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    Payload::Mode& mode,
    bool hasComplexType) {
  auto sourceBuffers = std::move(buffers);
  // allocate length buffer
  // Length buffer layout |buffer
  // unCompressedLength|buffers.size()|complexBuffer count|buffer1 size|buffer2
  // size
  auto result = arrow::AllocateResizableBuffer(
      (sourceBuffers.size() + 3 - hasComplexType) * sizeof(int64_t), pool);
  BOLT_CHECK(
      result.ok(),
      "Failed to allocate length buffer: {}",
      result.status().message());
  std::shared_ptr<arrow::Buffer> lengthBuffer = result.MoveValueUnsafe();
  auto lengthBufferPtr = (int64_t*)lengthBuffer->mutable_data();
  // caculate length
  int64_t originalLength = std::accumulate(
      sourceBuffers.begin(),
      hasComplexType ? sourceBuffers.end() - 1 : sourceBuffers.end(),
      0LL,
      [&](auto sum, const auto& buffer) {
        if (!buffer) {
          return sum;
        }
        return sum + buffer->size();
      });
  // write length buffer
  int64_t pos = 0;
  lengthBufferPtr[pos++] = originalLength;
  lengthBufferPtr[pos++] = sourceBuffers.size() - hasComplexType;
  lengthBufferPtr[pos++] = hasComplexType;
  // allocate value buffer
  auto originalResult = arrow::AllocateResizableBuffer(originalLength, pool);
  BOLT_CHECK(
      originalResult.ok(),
      "Failed to allocate value buffer: {}",
      originalResult.status().message());
  std::shared_ptr<arrow::ResizableBuffer> original =
      originalResult.MoveValueUnsafe();
  // concat
  int offset = 0;
  for (int64_t i = 0; i < sourceBuffers.size() - hasComplexType; ++i) {
    const auto& buffer = sourceBuffers[i];
    if (buffer == nullptr) {
      lengthBufferPtr[pos++] = kNullBuffer;
    } else if (buffer->size() == 0) {
      lengthBufferPtr[pos++] = kZeroLengthBuffer;
    } else {
      std::memcpy(
          original->mutable_data() + offset, buffer->data(), buffer->size());
      lengthBufferPtr[pos++] = buffer->size();
      offset += buffer->size();
    }
  }
  // assemble buffers
  buffers.emplace_back(lengthBuffer);
  buffers.emplace_back(original);
  if (hasComplexType) {
    buffers.emplace_back(sourceBuffers.back());
  }
}

arrow::Result<std::unique_ptr<BlockPayload>> BlockPayload::fromBuffers(
    Payload::Type payloadType,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    const std::vector<bool>* isValidityBuffer,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    Payload::Mode mode,
    bool hasComplexType) {
  if (mode == Payload::Mode::kRowVector &&
      (payloadType == Payload::Type::kCompressed ||
       payloadType == Payload::Type::kToBeCompressed) &&
      buffers.size() > 3) {
    concatBuffer(buffers, pool, codec, mode, hasComplexType);
  }
  if (payloadType == Payload::Type::kCompressed) {
    uint64_t compressionTime = 0;
    std::shared_ptr<arrow::ResizableBuffer> compressed;
    {
      bytedance::bolt::NanosecondTimer timer(&compressionTime);
      // Compress.
      // Compressed buffer layout: | buffer1 compressedLength | buffer1
      // uncompressedLength | buffer1 | ...
      const auto metadataLength = sizeof(int64_t) * 2 * buffers.size();
      int64_t totalCompressedLength = std::accumulate(
          buffers.begin(),
          buffers.end(),
          0LL,
          [&](auto sum, const auto& buffer) {
            if (!buffer) {
              return sum;
            }
            return sum +
                codec->MaxCompressedLen(buffer->size(), buffer->data());
          });
      const auto maxCompressedLength = metadataLength + totalCompressedLength;
      ARROW_ASSIGN_OR_RAISE(
          compressed,
          arrow::AllocateResizableBuffer(maxCompressedLength, pool));

      auto output = compressed->mutable_data();
      int64_t actualLength = 0;
      // Compress buffers one by one.
      for (auto& buffer : buffers) {
        auto availableLength = maxCompressedLength - actualLength;
        // Release buffer after compression.
        ARROW_ASSIGN_OR_RAISE(
            auto compressedSize,
            compressBuffer(std::move(buffer), output, availableLength, codec));
        output += compressedSize;
        actualLength += compressedSize;
      }

      ARROW_RETURN_IF(
          actualLength < 0,
          arrow::Status::Invalid("Writing compressed buffer out of bound."));
      RETURN_NOT_OK(compressed->Resize(actualLength));
    }
    auto payload = std::unique_ptr<BlockPayload>(new BlockPayload(
        Type::kCompressed,
        numRows,
        std::vector<std::shared_ptr<arrow::Buffer>>{compressed},
        isValidityBuffer,
        pool,
        codec,
        mode));
    payload->setCompressionTime(compressionTime);
    return payload;
  }
  return std::unique_ptr<BlockPayload>(new BlockPayload(
      payloadType,
      numRows,
      std::move(buffers),
      isValidityBuffer,
      pool,
      codec,
      mode));
}

arrow::Status BlockPayload::serialize(arrow::io::OutputStream* outputStream) {
  switch (type_) {
    case Type::kUncompressed: {
      bytedance::bolt::NanosecondTimer timer(&writeTime_);
      RETURN_NOT_OK(outputStream->Write(&kUncompressedType, sizeof(Type)));
      RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
      RETURN_NOT_OK(outputStream->Write(&kBufferMode, sizeof(Mode)));
      for (auto& buffer : buffers_) {
        if (!buffer) {
          RETURN_NOT_OK(outputStream->Write(&kNullBuffer, sizeof(int64_t)));
          continue;
        }
        int64_t bufferSize = buffer->size();
        RETURN_NOT_OK(outputStream->Write(&bufferSize, sizeof(int64_t)));
        if (bufferSize > 0) {
          RETURN_NOT_OK(outputStream->Write(std::move(buffer)));
        }
      }
    } break;
    case Type::kToBeCompressed: {
      {
        bytedance::bolt::NanosecondTimer timer(&writeTime_);
        RETURN_NOT_OK(outputStream->Write(&kCompressedType, sizeof(Type)));
        RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
        RETURN_NOT_OK(outputStream->Write(&mode_, sizeof(Mode)));
      }
      for (auto& buffer : buffers_) {
        RETURN_NOT_OK(compressAndFlush(
            std::move(buffer),
            outputStream,
            codec_,
            pool_,
            compressTime_,
            writeTime_));
      }
    } break;
    case Type::kCompressed: {
      bytedance::bolt::NanosecondTimer timer(&writeTime_);
      RETURN_NOT_OK(outputStream->Write(&kCompressedType, sizeof(Type)));
      RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
      RETURN_NOT_OK(outputStream->Write(&mode_, sizeof(Mode)));
      RETURN_NOT_OK(outputStream->Write(std::move(buffers_[0])));
    } break;
    default: {
      BOLT_CHECK(false, "unexpected type " + std::to_string(type_));
    } break;
  }
  buffers_.clear();
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> BlockPayload::readBufferAt(
    uint32_t pos) {
  if (type_ == Type::kCompressed) {
    return arrow::Status::Invalid(
        "Cannot read buffer from compressed BlockPayload.");
  }
  return std::move(buffers_[pos]);
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
BlockPayload::deserialize(
    arrow::io::InputStream* inputStream,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::MemoryPool* pool,
    uint32_t& numRows,
    uint64_t& decompressTime,
    std::optional<uint8_t>& payloadType,
    ByteBuffer* readAheadBuffer) {
  static const std::vector<std::shared_ptr<arrow::Buffer>> kEmptyBuffers{};
  std::tuple<uint32_t, uint8_t> rowsAndMode;
  uint8_t type = payloadType.value();
  BOLT_CHECK(
      type < Type::kPayloadTypeEnd,
      "unexpected payload type " + std::to_string(type));
  payloadType.reset();

  ARROW_ASSIGN_OR_RAISE(
      rowsAndMode, readRowsAndMode(inputStream, &readAheadBuffer));
  numRows = std::get<0>(rowsAndMode);
  auto mode = std::get<1>(rowsAndMode);
  if (mode == Mode::kRowVector) {
    ARROW_RETURN_IF(
        type != Type::kCompressed,
        arrow::Status::Invalid("RowVector mode payload must be compressed"));
    return deserializeRowVectorModeBuffers(
        inputStream, codec, pool, numRows, decompressTime, &readAheadBuffer);
  }

  auto fields = schema->fields();
  auto isCompressionEnabled = type == Type::kCompressed;

  auto readBuffer = [&]() {
    if (isCompressionEnabled) {
      return readCompressedBuffer(
          inputStream,
          codec,
          pool,
          decompressTime,
          &readAheadBuffer,
          bytedance::bolt::simd::kPadding);
    } else {
      return readUncompressedBuffer(
          inputStream, &readAheadBuffer, pool, bytedance::bolt::simd::kPadding);
    }
  };

  bool hasComplexDataType = false;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (const auto& field : fields) {
    auto fieldType = field->type()->id();
    switch (fieldType) {
      case arrow::NullType::type_id: {
        // BoltShuffleWriter doesn't append buffer for arrow::NullType, so just
        // break this case
        break;
      }
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
        hasComplexDataType = true;
      } break;
      default: {
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        break;
      }
    }
  }
  if (hasComplexDataType) {
    buffers.emplace_back();
    ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
  }
  return buffers;
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
BlockPayload::deserializeRowVectorModeBuffers(
    arrow::io::InputStream* inputStream,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::MemoryPool* pool,
    uint32_t& numRows,
    uint64_t& decompressTime,
    ByteBuffer** readAheadBuffer) {
  // lengthBuffer
  std::shared_ptr<arrow::Buffer> lengthBuffer, valueBuffer;
  ARROW_ASSIGN_OR_RAISE(
      lengthBuffer,
      readCompressedBuffer(
          inputStream, codec, pool, decompressTime, readAheadBuffer));
  ARROW_RETURN_IF(
      lengthBuffer == nullptr || lengthBuffer->data() == nullptr,
      arrow::Status::Invalid(
          "RowVector mode length buffer should not be nullptr"));

  ARROW_ASSIGN_OR_RAISE(
      valueBuffer,
      readCompressedBuffer(
          inputStream,
          codec,
          pool,
          decompressTime,
          readAheadBuffer,
          bytedance::bolt::simd::kPadding));
  ARROW_RETURN_IF(
      valueBuffer == nullptr,
      arrow::Status::Invalid(
          "RowVector mode value buffer should not be nullptr"));

  const int64_t* lengthPtr = (const int64_t*)(lengthBuffer->data());
  // see assembleBuffersRowVectorMode
  int64_t uncompressLength = lengthPtr[0];
  int64_t valueBufferLength = lengthPtr[1];
  int64_t complexTypeBuffer = lengthPtr[2];
  ARROW_RETURN_IF(
      uncompressLength != valueBuffer->size(),
      arrow::Status::Invalid(
          "uncompressLength " + std::to_string(uncompressLength) +
          " stored in lengthBuffer do not equal actual lenght " +
          std::to_string(valueBuffer->size())));
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  int64_t bufferOffset = 0;
  for (auto i = 3; i < valueBufferLength + 3; ++i) {
    if (lengthPtr[i] == kNullBuffer) {
      buffers.push_back(nullptr);
    } else if (lengthPtr[i] == kZeroLengthBuffer) {
      buffers.push_back(zeroLengthNullBuffer());
    } else {
      buffers.push_back(
          arrow::SliceBuffer(valueBuffer, bufferOffset, lengthPtr[i]));
      bufferOffset += lengthPtr[i];
    }
  }
  ARROW_RETURN_IF(
      bufferOffset != uncompressLength,
      arrow::Status::Invalid(
          "uncompressLength " + std::to_string(uncompressLength) +
          " stored in lengthBuffer do not equal accumulated length " +
          std::to_string(bufferOffset)));
  if (complexTypeBuffer) {
    buffers.emplace_back();
    ARROW_ASSIGN_OR_RAISE(
        buffers.back(),
        readCompressedBuffer(
            inputStream, codec, pool, decompressTime, readAheadBuffer));
  }
  return buffers;
}

void BlockPayload::setCompressionTime(int64_t compressionTime) {
  compressTime_ = compressionTime;
}

arrow::Status BlockPayload::getVectorLayout(
    arrow::io::InputStream* inputStream,
    uint8_t& type,
    int64_t& bytes) {
  ARROW_ASSIGN_OR_RAISE(bytes, inputStream->Read(sizeof(Payload::Type), &type));
  return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<InMemoryPayload>> InMemoryPayload::merge(
    std::unique_ptr<InMemoryPayload> source,
    std::unique_ptr<InMemoryPayload> append,
    arrow::MemoryPool* pool,
    int64_t rowvectorModeCompressionMinColumns,
    int64_t rowvectorModeCompressionMaxBufferSize) {
  auto mergedRows = source->numRows() + append->numRows();
  auto isValidityBuffer = source->isValidityBuffer();

  auto numBuffers = append->numBuffers();
  ARROW_RETURN_IF(
      numBuffers != source->numBuffers(),
      arrow::Status::Invalid("Number of merging buffers doesn't match."));
  std::vector<std::shared_ptr<arrow::Buffer>> merged;
  merged.resize(numBuffers);
  for (size_t i = 0; i < numBuffers; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto sourceBuffer, source->readBufferAt(i));
    ARROW_ASSIGN_OR_RAISE(auto appendBuffer, append->readBufferAt(i));
    if (isValidityBuffer->at(i)) {
      if (!sourceBuffer) {
        if (!appendBuffer) {
          merged[i] = nullptr;
        } else {
          ARROW_ASSIGN_OR_RAISE(
              auto buffer,
              arrow::AllocateResizableBuffer(
                  arrow::bit_util::BytesForBits(mergedRows), pool));
          // Source is null, fill all true.
          arrow::bit_util::SetBitsTo(
              buffer->mutable_data(), 0, source->numRows(), true);
          // Write append bits.
          arrow::internal::CopyBitmap(
              appendBuffer->data(),
              0,
              append->numRows(),
              buffer->mutable_data(),
              source->numRows());
          merged[i] = std::move(buffer);
        }
      } else {
        // Because sourceBuffer can be resized, need to save buffer size in
        // advance.
        auto sourceBufferSize = sourceBuffer->size();
        auto resizable =
            std::dynamic_pointer_cast<arrow::ResizableBuffer>(sourceBuffer);
        auto mergedBytes = arrow::bit_util::BytesForBits(mergedRows);
        if (resizable) {
          // If source is resizable, resize and reuse source.
          RETURN_NOT_OK(resizable->Resize(mergedBytes));
        } else {
          // Otherwise copy source.
          ARROW_ASSIGN_OR_RAISE(
              resizable, arrow::AllocateResizableBuffer(mergedBytes, pool));
          memcpy(
              resizable->mutable_data(),
              sourceBuffer->data(),
              sourceBufferSize);
        }
        if (!appendBuffer) {
          arrow::bit_util::SetBitsTo(
              resizable->mutable_data(),
              source->numRows(),
              append->numRows(),
              true);
        } else {
          arrow::internal::CopyBitmap(
              appendBuffer->data(),
              0,
              append->numRows(),
              resizable->mutable_data(),
              source->numRows());
        }
        merged[i] = std::move(resizable);
      }
    } else {
      if (appendBuffer->size() == 0) {
        merged[i] = std::move(sourceBuffer);
      } else {
        // Because sourceBuffer can be resized, need to save buffer size in
        // advance.
        auto sourceBufferSize = sourceBuffer->size();
        auto mergedSize = sourceBufferSize + appendBuffer->size();
        auto resizable =
            std::dynamic_pointer_cast<arrow::ResizableBuffer>(sourceBuffer);
        if (resizable) {
          // If source is resizable, resize and reuse source.
          RETURN_NOT_OK(resizable->Resize(mergedSize));
        } else {
          // Otherwise copy source.
          ARROW_ASSIGN_OR_RAISE(
              resizable, arrow::AllocateResizableBuffer(mergedSize, pool));
          memcpy(
              resizable->mutable_data(),
              sourceBuffer->data(),
              sourceBufferSize);
        }
        // Copy append.
        memcpy(
            resizable->mutable_data() + sourceBufferSize,
            appendBuffer->data(),
            appendBuffer->size());
        merged[i] = std::move(resizable);
      }
    }
  }
  bool isRowVectorCompress =
      merged.size() >= rowvectorModeCompressionMinColumns &&
      bytedance::bolt::shuffle::sparksql::getBufferSize(merged) <
          rowvectorModeCompressionMaxBufferSize;
  return std::make_unique<InMemoryPayload>(
      mergedRows, isValidityBuffer, std::move(merged), isRowVectorCompress);
}

arrow::Result<std::unique_ptr<BlockPayload>> InMemoryPayload::toBlockPayload(
    Payload::Type payloadType,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    bool hasComplexType) {
  return BlockPayload::fromBuffers(
      payloadType,
      numRows_,
      std::move(buffers_),
      isValidityBuffer_,
      pool,
      codec,
      mode_,
      hasComplexType);
}

arrow::Status InMemoryPayload::serialize(
    arrow::io::OutputStream* outputStream) {
  return arrow::Status::Invalid("Cannot serialize InMemoryPayload.");
}

arrow::Result<std::shared_ptr<arrow::Buffer>> InMemoryPayload::readBufferAt(
    uint32_t index) {
  return std::move(buffers_[index]);
}

int64_t InMemoryPayload::getBufferSize() const {
  return bytedance::bolt::shuffle::sparksql::getBufferSize(buffers_);
}

arrow::Status InMemoryPayload::copyBuffers(arrow::MemoryPool* pool) {
  for (auto& buffer : buffers_) {
    if (!buffer) {
      continue;
    }
    if (buffer->size() == 0) {
      buffer = zeroLengthNullBuffer();
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(
        auto copy, arrow::AllocateResizableBuffer(buffer->size(), pool));
    memcpy(copy->mutable_data(), buffer->data(), buffer->size());
    buffer = std::move(copy);
  }
  return arrow::Status::OK();
}

UncompressedDiskBlockPayload::UncompressedDiskBlockPayload(
    Type type,
    uint32_t numRows,
    const std::vector<bool>* isValidityBuffer,
    arrow::io::InputStream*& inputStream,
    uint64_t rawSize,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec)
    : Payload(type, numRows, isValidityBuffer),
      inputStream_(inputStream),
      rawSize_(rawSize),
      pool_(pool),
      codec_(codec) {}

arrow::Result<std::shared_ptr<arrow::Buffer>>
UncompressedDiskBlockPayload::readBufferAt(uint32_t index) {
  return arrow::Status::Invalid(
      "Cannot read buffer from UncompressedDiskBlockPayload.");
}

arrow::Status UncompressedDiskBlockPayload::serialize(
    arrow::io::OutputStream* outputStream) {
  if (codec_ == nullptr || type_ == Payload::kUncompressed) {
    ARROW_ASSIGN_OR_RAISE(auto block, inputStream_->Read(rawSize_));
    RETURN_NOT_OK(outputStream->Write(block));
    return arrow::Status::OK();
  }

  ARROW_RETURN_IF(
      type_ != Payload::kToBeCompressed,
      arrow::Status::Invalid(
          "Invalid payload type: " + std::to_string(type_) +
          ", should be either Payload::kUncompressed or Payload::kToBeCompressed"));
  ARROW_ASSIGN_OR_RAISE(auto startPos, inputStream_->Tell());
  auto typeAndRows = readTypeAndRows(inputStream_);
  // Discard type and rows.
  RETURN_NOT_OK(outputStream->Write(&kCompressedType, sizeof(kCompressedType)));
  RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
  RETURN_NOT_OK(outputStream->Write(&kBufferMode, sizeof(Mode)));
  auto readPos =
      startPos + sizeof(kUncompressedType) + sizeof(uint32_t) + sizeof(Mode);
  while (readPos - startPos < rawSize_) {
    ARROW_ASSIGN_OR_RAISE(auto uncompressed, readUncompressedBuffer());
    ARROW_ASSIGN_OR_RAISE(readPos, inputStream_->Tell());
    RETURN_NOT_OK(compressAndFlush(
        std::move(uncompressed),
        outputStream,
        codec_,
        pool_,
        compressTime_,
        writeTime_));
  }
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
UncompressedDiskBlockPayload::readUncompressedBuffer() {
  readPos_++;
  int64_t bufferLength;
  RETURN_NOT_OK(inputStream_->Read(sizeof(int64_t), &bufferLength));
  if (bufferLength == kNullBuffer) {
    return nullptr;
  }
  if (bufferLength == 0) {
    return zeroLengthNullBuffer();
  }
  ARROW_ASSIGN_OR_RAISE(auto buffer, inputStream_->Read(bufferLength));
  return buffer;
}

CompressedDiskBlockPayload::CompressedDiskBlockPayload(
    uint32_t numRows,
    const std::vector<bool>* isValidityBuffer,
    arrow::io::InputStream*& inputStream,
    uint64_t rawSize,
    arrow::MemoryPool* pool)
    : Payload(Type::kCompressed, numRows, isValidityBuffer),
      inputStream_(inputStream),
      rawSize_(rawSize),
      pool_(pool) {}

arrow::Status CompressedDiskBlockPayload::serialize(
    arrow::io::OutputStream* outputStream) {
  ARROW_ASSIGN_OR_RAISE(auto block, inputStream_->Read(rawSize_));
  RETURN_NOT_OK(outputStream->Write(block));
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
CompressedDiskBlockPayload::readBufferAt(uint32_t index) {
  return arrow::Status::Invalid(
      "Cannot read buffer from CompressedDiskBlockPayload.");
}

arrow::Status RowBlockPayload::serialize(
    arrow::io::OutputStream* outputStream) {
  RETURN_NOT_OK(
      codec_->CompressAndFlush(rows_, outputStream, rawSize_, layout_));
  return arrow::Status::OK();
}

arrow::Status RowBlockPayload::deserialize(
    arrow::io::InputStream* inputStream,
    uint8_t* dst,
    int32_t dstSize,
    int32_t& offset,
    ZstdStreamCodec* codec,
    std::vector<std::string_view>& outputRows,
    int32_t& remainingOutputLen,
    bool& eof,
    bool& layoutEnd,
    RowVectorLayout& layout,
    uint64_t& decompressTime) {
  {
    bytedance::bolt::NanosecondTimer timer(&decompressTime);
    RETURN_NOT_OK(codec->Decompress(
        inputStream,
        dst,
        dstSize,
        offset,
        remainingOutputLen,
        eof,
        layoutEnd,
        layout));
  }
  // total decompressed size is previous partial rowsize + current decompressed
  // size
  remainingOutputLen += offset;
  offset = 0;
  while (remainingOutputLen >= kRowSizeBytes) {
    auto rowSize = *(RowSizeType*)(dst + offset);
    if (remainingOutputLen >= kRowSizeBytes + rowSize) {
      outputRows.emplace_back(std::string_view(
          reinterpret_cast<const char*>(dst + offset + kRowSizeBytes),
          rowSize));
      offset += kRowSizeBytes + rowSize;
      remainingOutputLen -= kRowSizeBytes + rowSize;
    } else {
      break;
    }
  }

  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RowBlockPayload::readBufferAt(
    uint32_t index) {
  return arrow::Status::Invalid("Cannot read buffer from RowBlockPayload.");
}

CompressedDiskRowBlockPayload::CompressedDiskRowBlockPayload(
    uint32_t numRows,
    arrow::io::InputStream*& inputStream,
    uint64_t rawSize)
    : Payload(Type::kCompressed, numRows, nullptr),
      inputStream_(inputStream),
      rawSize_(rawSize) {}

arrow::Status CompressedDiskRowBlockPayload::serialize(
    arrow::io::OutputStream* outputStream) {
  ARROW_ASSIGN_OR_RAISE(auto block, inputStream_->Read(rawSize_));
  RETURN_NOT_OK(outputStream->Write(block));
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
CompressedDiskRowBlockPayload::readBufferAt(uint32_t index) {
  return arrow::Status::Invalid(
      "Cannot read buffer from CompressedDiskRowBlockPayload.");
}
} // namespace bytedance::bolt::shuffle::sparksql
