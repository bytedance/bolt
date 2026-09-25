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

#include "bolt/dwio/lance/NativeLanceLegacyBlob.h"

#include <limits>

#include <folly/lang/Bits.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLanceColumnCursor.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

using ArrayEncoding = ::lance::encodings::ArrayEncoding;
using ColumnMetadata = ::lance::file::v2::ColumnMetadata;

template <typename T>
T readLittleEndian(const char* data) {
  return folly::Endian::little(folly::loadUnaligned<T>(data));
}

const ArrayEncoding& unwrapNoNullEncoding(
    const ArrayEncoding& encoding,
    std::string_view role) {
  if (encoding.array_encoding_case() != ArrayEncoding::kNullable) {
    return encoding;
  }
  BOLT_CHECK_EQ(
      encoding.nullable().nullability_case(),
      ::lance::encodings::Nullable::kNoNulls,
      "Native Lance Blob reader requires non-null {} encoding",
      role);
  return encoding.nullable().no_nulls().values();
}

uint64_t fixedEncodingBitWidth(const ArrayEncoding& encoding) {
  const auto& values = unwrapNoNullEncoding(encoding, "descriptor");
  if (values.array_encoding_case() == ArrayEncoding::kFlat) {
    return values.flat().bits_per_value();
  }
  BOLT_CHECK_EQ(values.array_encoding_case(), ArrayEncoding::kBitpacked);
  return values.bitpacked().compressed_bits_per_value();
}

struct BlobDescriptor {
  uint64_t position;
  uint64_t size;
};

void decodeDescriptions(
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t outputOffset,
    const NativeLanceLegacyRead& read,
    BlobDescriptor* output) {
  BOLT_CHECK_EQ(encoding.array_encoding_case(), ArrayEncoding::kPackedStruct);
  const auto& packed = encoding.packed_struct();
  BOLT_CHECK_EQ(packed.inner_size(), 2);
  BOLT_CHECK(packed.has_buffer());
  for (const auto& child : packed.inner()) {
    BOLT_CHECK_EQ(fixedEncodingBitWidth(child), 64);
  }
  constexpr uint64_t kDescriptorBytes = 2 * sizeof(uint64_t);
  const auto descriptor = metadata.resolveBuffer(packed.buffer(), column, page);
  BOLT_CHECK_LE(localStart, descriptor.length / kDescriptorBytes);
  BOLT_CHECK_LE(localCount, descriptor.length / kDescriptorBytes - localStart);
  const auto bytes = read(
      descriptor.offset + localStart * kDescriptorBytes,
      localCount * kDescriptorBytes);
  for (uint64_t row = 0; row < localCount; ++row) {
    const auto* source = bytes->as<char>() + row * kDescriptorBytes;
    output[outputOffset + row] = {
        readLittleEndian<uint64_t>(source),
        readLittleEndian<uint64_t>(source + sizeof(uint64_t))};
  }
}

} // namespace

VectorPtr decodeLegacyBlobColumn(
    const TypePtr& type,
    std::string_view logicalType,
    uint32_t physicalColumn,
    uint64_t rowStart,
    uint64_t rowCount,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    dwio::common::BufferedInput& input,
    const NativeLanceLegacyRead& read) {
  BOLT_CHECK_EQ(type->kind(), TypeKind::VARBINARY);
  BOLT_CHECK_EQ(logicalType, "large_binary");
  auto result = BaseVector::create(type, rowCount, &pool);
  if (rowCount == 0) {
    return result;
  }

  auto descriptors = AlignedBuffer::allocate<BlobDescriptor>(rowCount, &pool);
  auto* rawDescriptors = descriptors->asMutable<BlobDescriptor>();
  const auto& column = metadata.column(physicalColumn);
  uint64_t outputOffset = 0;
  NativeLanceColumnCursor cursor(
      physicalColumn, metadata.pageRowStarts(physicalColumn));
  for (const auto& span : cursor.spans(rowStart, rowCount)) {
    decodeDescriptions(
        metadata.pageEncoding(physicalColumn, span.pageIndex),
        column,
        column.pages(span.pageIndex),
        metadata,
        span.localRowBegin,
        span.rowCount,
        outputOffset,
        read,
        rawDescriptors);
    outputOffset += span.rowCount;
  }
  BOLT_CHECK_EQ(outputOffset, rowCount);

  uint64_t payloadBytes = 0;
  struct PayloadRead {
    uint64_t row;
    uint64_t outputOffset;
    std::unique_ptr<dwio::common::SeekableInputStream> stream;
  };
  std::vector<PayloadRead, memory::StlAllocator<PayloadRead>> payloadReads{
      memory::StlAllocator<PayloadRead>(&pool)};
  payloadReads.reserve(rowCount);
  const auto fileSize = input.getReadFile()->size();
  for (uint64_t row = 0; row < rowCount; ++row) {
    const auto [position, size] = rawDescriptors[row];
    if (position == 1 && size == 0) {
      result->setNull(row, true);
      continue;
    }
    BOLT_CHECK_LE(
        size,
        static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
        "Lance Blob value exceeds Bolt StringView capacity");
    BOLT_CHECK_LE(position, fileSize);
    BOLT_CHECK_LE(size, fileSize - position);
    BOLT_CHECK_LE(payloadBytes, std::numeric_limits<uint64_t>::max() - size);
    if (size > 0) {
      payloadReads.push_back(
          {row, payloadBytes, input.enqueue({position, size})});
    }
    payloadBytes += size;
  }
  if (!payloadReads.empty()) {
    input.load(dwio::common::LogType::BLOCK);
  }
  auto payload = AlignedBuffer::allocate<char>(payloadBytes, &pool);
  auto* values = result->asFlatVector<StringView>();
  if (payloadBytes > 0) {
    values->addStringBuffer(payload);
  }
  for (auto& payloadRead : payloadReads) {
    const auto size = rawDescriptors[payloadRead.row].size;
    payloadRead.stream->readFully(
        payload->asMutable<char>() + payloadRead.outputOffset, size);
  }
  uint64_t payloadOffset = 0;
  for (uint64_t row = 0; row < rowCount; ++row) {
    const auto [position, size] = rawDescriptors[row];
    if (position == 1 && size == 0) {
      continue;
    }
    values->setNoCopy(
        row,
        size == 0 ? StringView()
                  : StringView(
                        payload->as<char>() + payloadOffset,
                        static_cast<int32_t>(size)));
    payloadOffset += size;
  }
  BOLT_CHECK_EQ(payloadOffset, payloadBytes);
  return result;
}

} // namespace bytedance::bolt::lance::reader
