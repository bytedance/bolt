/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/dwio/paimon/deletionvectors/DeletionFileReader.h"

#include "bolt/common/base/Crc.h"
#include "bolt/dwio/common/StreamUtil.h"
#include "bolt/dwio/paimon/deletionvectors/DeletionVector.h"
namespace bytedance::bolt::paimon {
namespace {
constexpr uint32_t MagincNumber = 1581511376;
constexpr int32_t LengthOfSizeAndCrc = 8;

uint32_t readBigEndianUint32(const uint8_t* buffer) {
  return (static_cast<uint32_t>(buffer[0]) << 24) |
      (static_cast<uint32_t>(buffer[1]) << 16) |
      (static_cast<uint32_t>(buffer[2]) << 8) |
      (static_cast<uint32_t>(buffer[3]));
}

uint32_t crc32(const void* data, int32_t size) {
  bits::Crc32 crc32{};
  crc32.process_bytes(data, size);
  return crc32.checksum();
}
} // namespace

DeletionFileReader::DeletionFileReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    Options options)
    : input_(std::move(input)), options_(options) {}

DeletionFileReader::~DeletionFileReader() = default;

void DeletionFileReader::getDeletionVector(
    uint32_t offset,
    uint32_t length,
    BufferPtr* buf) {
  if (!deletionVector_) {
    initDeletionVector();
  }

  deletionVector_->getDeletionVector(offset, length, buf);
}

// DeletionFile Reference:
// https://paimon.apache.org/docs/master/concepts/spec/tableindex/#deletion-vectors
void DeletionFileReader::initDeletionVector() {
  BOLT_CHECK(!deletionVector_);
  auto readLength = options_.size + LengthOfSizeAndCrc;
  auto stream = input_->read(
      options_.offset, readLength, dwio::common::LogType::PAIMON_DELETION_BIN);
  auto buffer = AlignedBuffer::
      allocate<uint8_t, AlignedBuffer::AllocationStrategy::kConservative>(
          readLength, options_.memoryPool);
  const char* bufferStart = nullptr;
  const char* bufferEnd = nullptr;
  dwio::common::readBytes(
      readLength,
      stream.get(),
      buffer->asMutable<void>(),
      bufferStart,
      bufferEnd);

  auto bufferData = buffer->as<uint8_t>();
  auto binSize = readBigEndianUint32(bufferData);
  BOLT_CHECK_EQ(options_.size, binSize, "Size of bin mismatch.");
  bufferData += 4;
  auto actualCrc32 = crc32(bufferData, binSize);
  auto expectedCrc32 = readBigEndianUint32(bufferData + binSize);
  BOLT_CHECK_EQ(expectedCrc32, actualCrc32, "Checksum of bin mismatch.");
  auto maginc = readBigEndianUint32(bufferData);
  BOLT_CHECK_EQ(MagincNumber, maginc, "Magic number mismatch.");
  bufferData += 4;
  deletionVector_ = DeletionVector::create(
      reinterpret_cast<const char*>(bufferData),
      binSize - 4,
      options_.memoryPool);
}
} // namespace bytedance::bolt::paimon
