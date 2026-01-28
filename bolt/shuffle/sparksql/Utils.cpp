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

#include "bolt/shuffle/sparksql/Utils.h"
#include <arrow/record_batch.h>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <thread>
#include "arrow/c/bridge.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"
namespace bytedance::bolt::shuffle::sparksql {

namespace {
arrow::Result<std::shared_ptr<arrow::Array>> makeNullBinaryArray(
    std::shared_ptr<arrow::DataType> type,
    arrow::MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto offsetBuffer,
      arrow::AllocateResizableBuffer(kSizeOfIpcOffsetBuffer << 1, pool));
  // set the first offset to 0, and set the value offset
  uint8_t* offsetaddr = offsetBuffer->mutable_data();
  memset(offsetaddr, 0, kSizeOfIpcOffsetBuffer);
  // second value offset 0
  memset(offsetaddr + kSizeOfIpcOffsetBuffer, 0, kSizeOfIpcOffsetBuffer);
  // If it is not compressed array, null valueBuffer
  // worked, but if compress, will core dump at buffer::size(), so replace by
  // kNullBuffer
  static std::shared_ptr<arrow::Buffer> kNullBuffer =
      std::make_shared<arrow::Buffer>(nullptr, 0);
  return arrow::MakeArray(arrow::ArrayData::Make(
      type, 1, {nullptr, std::move(offsetBuffer), kNullBuffer}));
}

arrow::Result<std::shared_ptr<arrow::Array>> makeBinaryArray(
    std::shared_ptr<arrow::DataType> type,
    std::shared_ptr<arrow::Buffer> valueBuffer,
    arrow::MemoryPool* pool) {
  if (valueBuffer == nullptr) {
    return makeNullBinaryArray(type, pool);
  }

  ARROW_ASSIGN_OR_RAISE(
      auto offsetBuffer,
      arrow::AllocateResizableBuffer(kSizeOfIpcOffsetBuffer << 1, pool));
  // set the first offset to 0, and set the value offset
  uint8_t* offsetaddr = offsetBuffer->mutable_data();
  memset(offsetaddr, 0, kSizeOfIpcOffsetBuffer);
  int64_t length = valueBuffer->size();
  memcpy(
      offsetaddr + kSizeOfIpcOffsetBuffer,
      reinterpret_cast<uint8_t*>(&length),
      kSizeOfIpcOffsetBuffer);
  return arrow::MakeArray(arrow::ArrayData::Make(
      type, 1, {nullptr, std::move(offsetBuffer), valueBuffer}));
}

// Length buffer layout |compressionMode|buffers.size()|buffer1
// unCompressedLength|buffer1 compressedLength| buffer2...
arrow::Status getLengthBufferAndValueBufferOneByOne(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool,
    Codec* codec,
    std::shared_ptr<arrow::ResizableBuffer>& lengthBuffer,
    std::shared_ptr<arrow::ResizableBuffer>& valueBuffer) {
  ARROW_ASSIGN_OR_RAISE(
      lengthBuffer,
      arrow::AllocateResizableBuffer(
          (1 + 1 + buffers.size() * 2) * sizeof(int64_t), pool));
  auto lengthBufferPtr = (int64_t*)(lengthBuffer->mutable_data());
  // Write compression mode.
  *lengthBufferPtr++ = CompressionMode::BUFFER;
  // Write number of buffers.
  *lengthBufferPtr++ = buffers.size();

  int64_t compressedBufferMaxSize = getMaxCompressedBufferSize(buffers, codec);
  ARROW_ASSIGN_OR_RAISE(
      valueBuffer,
      arrow::AllocateResizableBuffer(compressedBufferMaxSize, pool));
  int64_t compressValueOffset = 0;
  for (auto& buffer : buffers) {
    if (buffer != nullptr && buffer->size() != 0) {
      int64_t actualLength;
      int64_t maxLength = codec->maxCompressedLen(buffer->size());
      actualLength = codec->compress(
          buffer->data(),
          buffer->size(),
          valueBuffer->mutable_data() + compressValueOffset,
          maxLength);
      compressValueOffset += actualLength;
      *lengthBufferPtr++ = buffer->size();
      *lengthBufferPtr++ = actualLength;
    } else {
      *lengthBufferPtr++ = 0;
      *lengthBufferPtr++ = 0;
    }
  }
  RETURN_NOT_OK(valueBuffer->Resize(compressValueOffset, /*shrink*/ true));
  return arrow::Status::OK();
}

// Length buffer layout |compressionMode|buffer unCompressedLength|buffer
// compressedLength|buffers.size()| buffer1 size | buffer2 size
arrow::Status getLengthBufferAndValueBufferStream(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool,
    Codec* codec,
    std::shared_ptr<arrow::ResizableBuffer>& lengthBuffer,
    std::shared_ptr<arrow::ResizableBuffer>& compressedBuffer) {
  ARROW_ASSIGN_OR_RAISE(
      lengthBuffer,
      arrow::AllocateResizableBuffer(
          (1 + 3 + buffers.size()) * sizeof(int64_t), pool));
  auto originalBufferSize = getBufferSize(buffers);

  // because 64B align, uncompressedBuffer size maybe bigger than
  // unCompressedBufferSize which is getBuffersSize(buffers), then cannot use
  // this size
  ARROW_ASSIGN_OR_RAISE(
      auto uncompressedBuffer,
      arrow::AllocateResizableBuffer(originalBufferSize, pool));
  int64_t uncompressedSize = uncompressedBuffer->size();

  auto lengthBufferPtr = (int64_t*)(lengthBuffer->mutable_data());
  // First write metadata.
  // Write compression mode.
  *lengthBufferPtr++ = CompressionMode::ROWVECTOR;
  // Store uncompressed size.
  *lengthBufferPtr++ = uncompressedSize; // uncompressedLength
  // Skip compressed size and update later.
  auto compressedLengthPtr = lengthBufferPtr++;
  // Store number of buffers.
  *lengthBufferPtr++ = buffers.size();

  int64_t compressValueOffset = 0;
  for (auto& buffer : buffers) {
    // Copy all buffers into one big buffer.
    if (buffer != nullptr && buffer->size() != 0) {
      *lengthBufferPtr++ = buffer->size();
      memcpy(
          uncompressedBuffer->mutable_data() + compressValueOffset,
          buffer->data(),
          buffer->size());
      compressValueOffset += buffer->size();
    } else {
      *lengthBufferPtr++ = 0;
    }
  }

  // Compress the big buffer.
  int64_t maxLength = codec->maxCompressedLen(uncompressedSize);
  ARROW_ASSIGN_OR_RAISE(
      compressedBuffer, arrow::AllocateResizableBuffer(maxLength, pool));
  int64_t actualLength = codec->compress(
      uncompressedBuffer->data(),
      uncompressedSize,
      compressedBuffer->mutable_data(),
      maxLength);
  RETURN_NOT_OK(compressedBuffer->Resize(actualLength, /*shrink*/ true));

  // Update compressed size.
  *compressedLengthPtr = actualLength;
  return arrow::Status::OK();
}
} // namespace

std::string generateUuid() {
  boost::uuids::random_generator generator;
  return boost::uuids::to_string(generator());
}

std::string getSpilledShuffleFileDir(
    const std::string& configuredDir,
    int32_t subDirId) {
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(2) << std::hex << subDirId;
  auto dir = arrow::fs::internal::ConcatAbstractPath(configuredDir, ss.str());
  return dir;
}

arrow::Result<std::string> createTempShuffleFile(const std::string& dir) {
  if (dir.length() == 0) {
    return arrow::Status::Invalid(
        "Failed to create spilled file, got empty path.");
  }

  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ARROW_ASSIGN_OR_RAISE(auto path_info, fs->GetFileInfo(dir));
  if (path_info.type() == arrow::fs::FileType::NotFound) {
    RETURN_NOT_OK(fs->CreateDir(dir, true));
  }

  bool exist = true;
  std::string filePath;
  while (exist) {
    filePath = arrow::fs::internal::ConcatAbstractPath(
        dir, "temp_shuffle_" + generateUuid());
    ARROW_ASSIGN_OR_RAISE(auto file_info, fs->GetFileInfo(filePath));
    if (file_info.type() == arrow::fs::FileType::NotFound) {
      int fd = open(filePath.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);
      if (fd < 0) {
        if (errno != EEXIST) {
          return arrow::Status::IOError(
              "Failed to open local file " + filePath +
              ", Reason: " + strerror(errno));
        }
      } else {
        exist = false;
        close(fd);
      }
    }
  }
  return filePath;
}

arrow::Result<std::vector<std::shared_ptr<arrow::DataType>>> toShuffleTypeId(
    const std::vector<std::shared_ptr<arrow::Field>>& fields) {
  std::vector<std::shared_ptr<arrow::DataType>> shuffleTypeId;
  for (auto field : fields) {
    switch (field->type()->id()) {
      case arrow::BooleanType::type_id:
      case arrow::Int8Type::type_id:
      case arrow::UInt8Type::type_id:
      case arrow::Int16Type::type_id:
      case arrow::UInt16Type::type_id:
      case arrow::HalfFloatType::type_id:
      case arrow::Int32Type::type_id:
      case arrow::UInt32Type::type_id:
      case arrow::FloatType::type_id:
      case arrow::Date32Type::type_id:
      case arrow::Time32Type::type_id:
      case arrow::Int64Type::type_id:
      case arrow::UInt64Type::type_id:
      case arrow::DoubleType::type_id:
      case arrow::Date64Type::type_id:
      case arrow::Time64Type::type_id:
      case arrow::TimestampType::type_id:
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::Decimal128Type::type_id:
      case arrow::NullType::type_id:
      case arrow::MonthIntervalType::type_id:
        shuffleTypeId.push_back(field->type());
        break;
      default:
        RETURN_NOT_OK(arrow::Status::NotImplemented(
            "Field type not implemented in ColumnarShuffle, type is ",
            field->type()->ToString()));
    }
  }
  return shuffleTypeId;
}

int64_t getBufferSize(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  return std::accumulate(
      std::cbegin(buffers),
      std::cend(buffers),
      0LL,
      [](int64_t sum, const std::shared_ptr<arrow::Buffer>& buf) {
        return buf == nullptr ? sum : sum + buf->size();
      });
}

int64_t getBufferSize(const std::shared_ptr<arrow::Array>& array) {
  return getBufferSize(array->data()->buffers);
}

int64_t getMaxCompressedBufferSize(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    Codec* codec) {
  int64_t totalSize = 0;
  for (auto& buffer : buffers) {
    if (buffer != nullptr && buffer->size() != 0) {
      totalSize += codec->maxCompressedLen(buffer->size());
    }
  }
  return totalSize;
}

std::shared_ptr<arrow::Buffer> zeroLengthNullBuffer() {
  static std::shared_ptr<arrow::Buffer> kNullBuffer =
      std::make_shared<arrow::Buffer>(nullptr, 0);
  return kNullBuffer;
}

std::shared_ptr<arrow::Schema> boltTypeToArrowSchema(
    const bytedance::bolt::TypePtr& rowType,
    bytedance::bolt::memory::MemoryPool* pool) {
  ArrowSchema arrowSchema;
  ArrowOptions options;
  options.timestampUnit = static_cast<TimestampUnit>(6);
  exportToArrow(
      bytedance::bolt::BaseVector::create(rowType, 0, pool),
      arrowSchema,
      options);
  auto outputSchema = arrow::ImportSchema(&arrowSchema);
  BOLT_CHECK(
      outputSchema.ok(),
      "Failed to convert Bolt type to Arrow schema: {}",
      outputSchema.status().ToString());
  return outputSchema.ValueOrDie();
}

} // namespace bytedance::bolt::shuffle::sparksql
