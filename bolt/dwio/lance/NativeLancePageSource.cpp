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

#include "bolt/dwio/lance/NativeLancePageSource.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <functional>
#include <limits>

#include <folly/Portability.h>
#include <folly/lang/Bits.h>
#include <google/protobuf/any.pb.h>
#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLanceBitmap.h"
#include "bolt/dwio/lance/NativeLanceBitpack.h"
#include "bolt/dwio/lance/NativeLanceColumnCursor.h"
#include "bolt/dwio/lance/NativeLanceColumnReader.h"
#include "bolt/dwio/lance/NativeLanceDecompressor.h"
#include "bolt/dwio/lance/NativeLanceLegacyBinary.h"
#include "bolt/dwio/lance/NativeLanceLegacyBlob.h"
#include "bolt/dwio/lance/NativeLanceLegacyDictionary.h"
#include "bolt/dwio/lance/NativeLanceLegacyList.h"
#include "bolt/dwio/lance/NativeLanceLegacyScalar.h"
#include "bolt/dwio/lance/NativeLanceLegacyStruct.h"
#include "bolt/dwio/lance/NativeLanceListOffsets.h"
#include "bolt/dwio/lance/NativeLancePackedStruct.h"
#include "bolt/dwio/lance/NativeLancePageReader.h"
#include "bolt/dwio/lance/NativeLanceStructuralDecoder.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"
#include "bolt/dwio/lance/proto/lance_encodings_v2_0.pb.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

using ArrayEncoding = ::lance::encodings::ArrayEncoding;
using BufferDescriptor = NativeLanceMetadata::BufferDescriptor;
using ColumnMetadata = ::lance::file::v2::ColumnMetadata;

class DecodeInput {
 public:
  using RawRead = std::function<BufferPtr(uint64_t, uint64_t)>;
  using CompressedRead = std::function<
      BufferPtr(std::string_view, uint64_t, uint64_t, uint64_t, uint64_t)>;

  DecodeInput(RawRead rawRead, CompressedRead compressedRead)
      : rawRead_(std::move(rawRead)),
        compressedRead_(std::move(compressedRead)) {}

  operator const RawRead&() const {
    return rawRead_;
  }

  const RawRead& rawRead() const {
    return rawRead_;
  }

  const CompressedRead& compressedRead() const {
    return compressedRead_;
  }

 private:
  RawRead rawRead_;
  CompressedRead compressedRead_;
};

const ::lance::encodings::Flat& requireFlat(
    const ArrayEncoding& encoding,
    std::string_view role);
bool isCompressed(const ::lance::encodings::Flat& flat);
bool hasCompressedFlatBuffer(const ArrayEncoding& encoding);

bool requiresDeferredRead(const TypePtr& type) {
  if (type->kind() == TypeKind::VARCHAR ||
      type->kind() == TypeKind::VARBINARY || type->kind() == TypeKind::ARRAY ||
      type->kind() == TypeKind::MAP) {
    return true;
  }
  if (type->kind() == TypeKind::ROW) {
    for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
      if (requiresDeferredRead(type->childAt(childIndex))) {
        return true;
      }
    }
  }
  return false;
}

uint64_t fixedEncodingBitWidth(const ArrayEncoding& encoding) {
  switch (encoding.array_encoding_case()) {
    case ArrayEncoding::kFlat:
      return encoding.flat().bits_per_value();
    case ArrayEncoding::kBitpacked:
      return encoding.bitpacked().compressed_bits_per_value();
    case ArrayEncoding::kFixedSizeBinary:
      return static_cast<uint64_t>(encoding.fixed_size_binary().byte_width()) *
          8;
    case ArrayEncoding::kFixedSizeList:
      BOLT_CHECK(encoding.fixed_size_list().has_items());
      return static_cast<uint64_t>(encoding.fixed_size_list().dimension()) *
          fixedEncodingBitWidth(encoding.fixed_size_list().items());
    case ArrayEncoding::kNullable:
      BOLT_CHECK_EQ(
          encoding.nullable().nullability_case(),
          ::lance::encodings::Nullable::kNoNulls,
          "Packed encoding cannot contain nullable values");
      return fixedEncodingBitWidth(encoding.nullable().no_nulls().values());
    default:
      BOLT_UNSUPPORTED(
          "Encoding {} does not have a fixed bit width",
          static_cast<int>(encoding.array_encoding_case()));
  }
}

void enqueueEncodingRanges(
    const ArrayEncoding& encoding,
    const ColumnMetadata& column,
    const ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t elementStart,
    uint64_t elementCount,
    const std::function<void(uint64_t, uint64_t)>& enqueue,
    const std::function<void(std::string_view, BufferDescriptor)>&
        enqueueCompressed) {
  auto enqueueBuffer = [&](const ::lance::encodings::Buffer& encodedBuffer,
                           uint64_t bitWidth,
                           uint64_t start,
                           uint64_t count,
                           bool compressed) {
    BOLT_CHECK_GT(bitWidth, 0);
    const auto buffer = metadata.resolveBuffer(encodedBuffer, column, page);
    if (compressed) {
      if (buffer.length > 0) {
        enqueueCompressed(encoding.flat().compression().scheme(), buffer);
      }
      return;
    }
    BOLT_CHECK_LE(start, std::numeric_limits<uint64_t>::max() / bitWidth);
    BOLT_CHECK_LE(
        count,
        (std::numeric_limits<uint64_t>::max() - start * bitWidth) / bitWidth);
    const auto firstBit = start * bitWidth;
    const auto endBit = (start + count) * bitWidth;
    const auto firstByte = firstBit / 8;
    const auto endByte = (endBit + 7) / 8;
    BOLT_CHECK_LE(firstByte, buffer.length);
    BOLT_CHECK_LE(endByte, buffer.length);
    if (endByte > firstByte) {
      enqueue(buffer.offset + firstByte, endByte - firstByte);
    }
  };

  switch (encoding.array_encoding_case()) {
    case ArrayEncoding::kFlat: {
      const auto& flat = requireFlat(encoding, "prefetch");
      enqueueBuffer(
          flat.buffer(),
          flat.bits_per_value(),
          elementStart,
          elementCount,
          isCompressed(flat));
      return;
    }
    case ArrayEncoding::kBitpacked: {
      const auto& bitpacked = encoding.bitpacked();
      if (bitpacked.compressed_bits_per_value() == 0) {
        return;
      }
      BOLT_CHECK(bitpacked.has_buffer());
      enqueueBuffer(
          bitpacked.buffer(),
          bitpacked.compressed_bits_per_value(),
          elementStart,
          elementCount,
          false);
      return;
    }
    case ArrayEncoding::kBitpackedForNonNeg: {
      constexpr uint64_t kChunkRows = 1'024;
      const auto& bitpacked = encoding.bitpacked_for_non_neg();
      if (bitpacked.compressed_bits_per_value() == 0) {
        return;
      }
      BOLT_CHECK(bitpacked.has_buffer());
      const auto descriptor =
          metadata.resolveBuffer(bitpacked.buffer(), column, page);
      const auto chunkBytes =
          kChunkRows * bitpacked.compressed_bits_per_value() / 8;
      const auto firstChunk = elementStart / kChunkRows;
      const auto endChunk =
          (elementStart + elementCount + kChunkRows - 1) / kChunkRows;
      BOLT_CHECK_LE(firstChunk * chunkBytes, descriptor.length);
      BOLT_CHECK_LE(endChunk * chunkBytes, descriptor.length);
      if (endChunk > firstChunk) {
        enqueue(
            descriptor.offset + firstChunk * chunkBytes,
            (endChunk - firstChunk) * chunkBytes);
      }
      return;
    }
    case ArrayEncoding::kNullable: {
      const auto& nullable = encoding.nullable();
      if (nullable.nullability_case() ==
          ::lance::encodings::Nullable::kSomeNulls) {
        enqueueEncodingRanges(
            nullable.some_nulls().validity(),
            column,
            page,
            metadata,
            elementStart,
            elementCount,
            enqueue,
            enqueueCompressed);
        enqueueEncodingRanges(
            nullable.some_nulls().values(),
            column,
            page,
            metadata,
            elementStart,
            elementCount,
            enqueue,
            enqueueCompressed);
      } else if (
          nullable.nullability_case() ==
          ::lance::encodings::Nullable::kNoNulls) {
        enqueueEncodingRanges(
            nullable.no_nulls().values(),
            column,
            page,
            metadata,
            elementStart,
            elementCount,
            enqueue,
            enqueueCompressed);
      }
      return;
    }
    case ArrayEncoding::kBinary: {
      const auto firstIndex = elementStart == 0 ? 0 : elementStart - 1;
      enqueueEncodingRanges(
          encoding.binary().indices(),
          column,
          page,
          metadata,
          firstIndex,
          elementCount + (elementStart == 0 ? 0 : 1),
          enqueue,
          enqueueCompressed);
      return;
    }
    case ArrayEncoding::kList: {
      const auto firstIndex = elementStart == 0 ? 0 : elementStart - 1;
      enqueueEncodingRanges(
          encoding.list().offsets(),
          column,
          page,
          metadata,
          firstIndex,
          elementCount + (elementStart == 0 ? 0 : 1),
          enqueue,
          enqueueCompressed);
      return;
    }
    case ArrayEncoding::kDictionary:
      enqueueEncodingRanges(
          encoding.dictionary().indices(),
          column,
          page,
          metadata,
          elementStart,
          elementCount,
          enqueue,
          enqueueCompressed);
      enqueueEncodingRanges(
          encoding.dictionary().items(),
          column,
          page,
          metadata,
          0,
          encoding.dictionary().num_dictionary_items(),
          enqueue,
          enqueueCompressed);
      return;
    case ArrayEncoding::kFsst:
      enqueueEncodingRanges(
          encoding.fsst().binary(),
          column,
          page,
          metadata,
          elementStart,
          elementCount,
          enqueue,
          enqueueCompressed);
      return;
    case ArrayEncoding::kFixedSizeBinary:
      enqueueEncodingRanges(
          encoding.fixed_size_binary().bytes(),
          column,
          page,
          metadata,
          elementStart,
          elementCount,
          enqueue,
          enqueueCompressed);
      return;
    case ArrayEncoding::kFixedSizeList:
      enqueueEncodingRanges(
          encoding.fixed_size_list().items(),
          column,
          page,
          metadata,
          elementStart * encoding.fixed_size_list().dimension(),
          elementCount * encoding.fixed_size_list().dimension(),
          enqueue,
          enqueueCompressed);
      return;
    case ArrayEncoding::kPackedStruct: {
      BOLT_CHECK(encoding.packed_struct().has_buffer());
      uint64_t bitsPerRow = 0;
      for (const auto& child : encoding.packed_struct().inner()) {
        bitsPerRow += fixedEncodingBitWidth(child);
      }
      enqueueBuffer(
          encoding.packed_struct().buffer(),
          bitsPerRow,
          elementStart,
          elementCount,
          false);
      return;
    }
    case ArrayEncoding::kStruct:
      return;
    default:
      // Unsupported encodings are rejected by the decoder with a precise
      // error. Avoid turning a best-effort prefetch into an earlier failure.
      return;
  }
}

const ::lance::encodings::Flat& requireFlat(
    const ArrayEncoding& encoding,
    std::string_view role) {
  BOLT_CHECK_EQ(
      encoding.array_encoding_case(),
      ArrayEncoding::kFlat,
      "Native Lance fixed-width decoder requires Flat {} encoding",
      role);
  const auto& flat = encoding.flat();
  BOLT_CHECK(flat.has_buffer(), "Lance Flat {} encoding has no buffer", role);
  return flat;
}

bool isCompressed(const ::lance::encodings::Flat& flat) {
  return flat.has_compression() && !flat.compression().scheme().empty() &&
      flat.compression().scheme() != "none";
}

bool hasCompressedFlatBuffer(const ArrayEncoding& encoding) {
  switch (encoding.array_encoding_case()) {
    case ArrayEncoding::kFlat:
      return isCompressed(encoding.flat());
    case ArrayEncoding::kNullable: {
      const auto& nullable = encoding.nullable();
      if (nullable.nullability_case() ==
          ::lance::encodings::Nullable::kSomeNulls) {
        return hasCompressedFlatBuffer(nullable.some_nulls().validity()) ||
            hasCompressedFlatBuffer(nullable.some_nulls().values());
      }
      if (nullable.nullability_case() ==
          ::lance::encodings::Nullable::kNoNulls) {
        return hasCompressedFlatBuffer(nullable.no_nulls().values());
      }
      return false;
    }
    case ArrayEncoding::kBinary:
      return hasCompressedFlatBuffer(encoding.binary().indices()) ||
          hasCompressedFlatBuffer(encoding.binary().bytes());
    case ArrayEncoding::kList:
      return hasCompressedFlatBuffer(encoding.list().offsets());
    case ArrayEncoding::kDictionary:
      return hasCompressedFlatBuffer(encoding.dictionary().indices()) ||
          hasCompressedFlatBuffer(encoding.dictionary().items());
    case ArrayEncoding::kFsst:
      return hasCompressedFlatBuffer(encoding.fsst().binary());
    case ArrayEncoding::kFixedSizeBinary:
      return hasCompressedFlatBuffer(encoding.fixed_size_binary().bytes());
    case ArrayEncoding::kFixedSizeList:
      return hasCompressedFlatBuffer(encoding.fixed_size_list().items());
    case ArrayEncoding::kPackedStruct:
      return false;
    case ArrayEncoding::kStruct:
      return false;
    default:
      return false;
  }
}

} // namespace

NativeLancePageSource::NativeLancePageSource(
    dwio::common::BufferedInput& input,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
    NativeLanceReadScheduler::Options readSchedulerOptions)
    : input_(input),
      metadata_(metadata),
      pool_(pool),
      blobResolver_(std::move(blobResolver)),
      compressedStreamChunkBytes_(std::min(
          readSchedulerOptions.maxReadBytes,
          readSchedulerOptions.maxInFlightBytes)),
      readScheduler_(pool, readSchedulerOptions),
      structuralPagePlans_(metadata.numPhysicalColumns()) {}

NativeLancePageSource::~NativeLancePageSource() {
  cancel();
}

void NativeLancePageSource::cancel() {
  {
    std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
    readScheduler_.cancel(&input_);
  }
  std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
  for (auto& [key, session] : legacyPageReaders_) {
    session->cancel();
  }
  legacyPageReaders_.clear();
  {
    std::lock_guard<std::mutex> planLock(structuralPagePlansMutex_);
    for (auto& plans : structuralPagePlans_) {
      plans.clear();
    }
  }
}

void NativeLancePageSource::prefetchColumns(
    const std::vector<uint32_t>& columnIndices,
    uint64_t rowStart,
    uint64_t rowCount) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  planColumns(columnIndices, rowStart, rowCount);
  materializeReadPlan();
}

void NativeLancePageSource::planColumns(
    const std::vector<uint32_t>& columnIndices,
    const NativeLanceColumnRequest& request) const {
  request.validate();
  if (request.selection.selectsAll()) {
    planColumns(columnIndices, request.rowStart, request.rowCount);
    return;
  }
  if (input_.supportSyncLoad()) {
    return;
  }

  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  BOLT_CHECK_LE(request.rowStart, metadata_.numRows());
  BOLT_CHECK_LE(request.rowCount, metadata_.numRows() - request.rowStart);
  readScheduler_.clearStage();
  std::vector<StructuralPageRangeRequest> structuralRequests;
  const auto rows = request.selection.selectedRows();
  size_t runOffset = 0;
  while (runOffset < rows.size()) {
    size_t runEnd = runOffset + 1;
    while (runEnd < rows.size() && rows[runEnd] == rows[runEnd - 1] + 1) {
      ++runEnd;
    }
    for (const auto columnIndex : columnIndices) {
      enqueueLogicalColumn(
          columnIndex,
          request.rowStart + rows[runOffset],
          runEnd - runOffset,
          &structuralRequests);
    }
    runOffset = runEnd;
  }
  submitReadPlan();
  scheduleStructuralPayloads(structuralRequests);
  submitReadPlan();
}

void NativeLancePageSource::planColumns(
    const std::vector<uint32_t>& columnIndices,
    uint64_t rowStart,
    uint64_t rowCount) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  BOLT_CHECK_LE(rowStart, metadata_.numRows());
  BOLT_CHECK_LE(rowCount, metadata_.numRows() - rowStart);
  if (rowCount == 0) {
    return;
  }

  readScheduler_.clearStage();
  std::vector<StructuralPageRangeRequest> structuralRequests;
  for (const auto logicalColumnIndex : columnIndices) {
    enqueueLogicalColumn(
        logicalColumnIndex, rowStart, rowCount, &structuralRequests);
  }
  submitReadPlan();
  scheduleStructuralPayloads(structuralRequests);
  submitReadPlan();
}

void NativeLancePageSource::enqueueLogicalColumn(
    uint32_t columnIndex,
    uint64_t rowStart,
    uint64_t rowCount,
    std::vector<StructuralPageRangeRequest>* structuralRequests) const {
  BOLT_CHECK_LT(columnIndex, metadata_.rowType()->size());
  if (metadata_.usesStructuralEncoding()) {
    enqueueStructuralField(
        metadata_.structuralField(columnIndex),
        rowStart,
        rowCount,
        structuralRequests);
    return;
  }
  enqueuePhysicalColumn(
      metadata_.rowType()->childAt(columnIndex),
      metadata_.physicalColumnIndex(columnIndex),
      rowStart,
      rowCount,
      {},
      structuralRequests);
}

void NativeLancePageSource::enqueueStructuralField(
    const NativeLanceMetadata::StructuralField& field,
    uint64_t rowStart,
    uint64_t rowCount,
    std::vector<StructuralPageRangeRequest>* structuralRequests) const {
  if (!field.leaf) {
    for (const auto& child : field.children) {
      enqueueStructuralField(child, rowStart, rowCount, structuralRequests);
    }
    return;
  }
  BOLT_CHECK_LE(
      rowStart, std::numeric_limits<uint64_t>::max() / field.rowsPerParent);
  BOLT_CHECK_LE(
      rowCount, std::numeric_limits<uint64_t>::max() / field.rowsPerParent);
  enqueuePhysicalColumn(
      field.type,
      field.physicalColumnIndex,
      rowStart * field.rowsPerParent,
      rowCount * field.rowsPerParent,
      field.rowsPerParent == 1 ? std::vector<uint32_t>{}
                               : std::vector<uint32_t>{1},
      structuralRequests);
}

void NativeLancePageSource::prefetchPhysicalColumn(
    const TypePtr& type,
    uint32_t physicalColumnIndex,
    uint64_t rowStart,
    uint64_t rowCount) const {
  prefetchPhysicalColumns({{type, physicalColumnIndex}}, rowStart, rowCount);
}

void NativeLancePageSource::prefetchPhysicalColumns(
    const std::vector<std::pair<TypePtr, uint32_t>>& columns,
    uint64_t rowStart,
    uint64_t rowCount) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  planPhysicalColumns(columns, rowStart, rowCount);
  materializeReadPlan();
}

void NativeLancePageSource::planPhysicalColumns(
    const std::vector<std::pair<TypePtr, uint32_t>>& columns,
    uint64_t rowStart,
    uint64_t rowCount) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  readScheduler_.clearStage();
  std::vector<StructuralPageRangeRequest> structuralRequests;
  for (const auto& [type, physicalColumnIndex] : columns) {
    enqueuePhysicalColumn(
        type, physicalColumnIndex, rowStart, rowCount, {}, &structuralRequests);
  }
  submitReadPlan();
  scheduleStructuralPayloads(structuralRequests);
  submitReadPlan();
}

void NativeLancePageSource::enqueuePhysicalColumn(
    const TypePtr& type,
    uint32_t physicalIndex,
    uint64_t rowStart,
    uint64_t rowCount,
    const std::vector<uint32_t>& arrayDimensions,
    std::vector<StructuralPageRangeRequest>* structuralRequests) const {
  BOLT_CHECK_LT(physicalIndex, metadata_.numPhysicalColumns());
  if (metadata_.usesStructuralEncoding()) {
    const auto& column = metadata_.column(physicalIndex);
    NativeLanceColumnCursor cursor(
        physicalIndex, metadata_.pageRowStarts(physicalIndex));
    for (const auto& span : cursor.spans(rowStart, rowCount)) {
      const auto& page = column.pages(span.pageIndex);
      const auto& layout = metadata_.pageLayout(physicalIndex, span.pageIndex);
      const auto rangeRead = lanceStructuralPageSupportsRangeRead(
          type,
          arrayDimensions,
          metadata_.physicalColumnChildLogicalTypes(physicalIndex),
          layout);
      const NativeLancePageKey pageKey{physicalIndex, span.pageIndex};
      const auto preparedPlan = rangeRead
          ? findStructuralPagePlan(pageKey)
          : std::shared_ptr<const NativeLanceStructuralPagePlan>{};
      if (preparedPlan != nullptr) {
        if (!input_.supportSyncLoad()) {
          for (const auto& [offset, length] :
               preparedPlan->payloadRanges(span.localRowBegin, span.rowCount)) {
            scheduleRead(offset, length);
          }
        }
        continue;
      }
      for (int32_t buffer = 0; buffer < page.buffer_offsets_size(); ++buffer) {
        if (rangeRead &&
            ((layout.layout_case() ==
                  ::lance::encodings21::PageLayout::kMiniBlockLayout &&
              buffer == 1) ||
             layout.layout_case() ==
                 ::lance::encodings21::PageLayout::kFullZipLayout)) {
          continue;
        }
        if (page.buffer_sizes(buffer) > 0) {
          scheduleRead(page.buffer_offsets(buffer), page.buffer_sizes(buffer));
        }
      }
      if (rangeRead &&
          layout.layout_case() ==
              ::lance::encodings21::PageLayout::kMiniBlockLayout &&
          structuralRequests != nullptr) {
        structuralRequests->push_back(
            {.physicalColumnIndex = physicalIndex,
             .pageIndex = span.pageIndex,
             .localRowStart = span.localRowBegin,
             .rowCount = span.rowCount});
      }
    }
    return;
  }
  auto enqueueCompressed =
      [this](std::string_view scheme, BufferDescriptor buffer) {
        scheduleCompressedRead(scheme, buffer.offset, buffer.length);
      };
  if (metadata_.isBlobColumn(physicalIndex)) {
    const auto& column = metadata_.column(physicalIndex);
    auto enqueue = [this](uint64_t offset, uint64_t length) {
      scheduleRead(offset, length);
    };
    NativeLanceColumnCursor cursor(
        physicalIndex, metadata_.pageRowStarts(physicalIndex));
    for (const auto& span : cursor.spans(rowStart, rowCount)) {
      const auto& page = column.pages(span.pageIndex);
      const auto& encoding =
          metadata_.pageEncoding(physicalIndex, span.pageIndex);
      enqueueEncodingRanges(
          encoding,
          column,
          page,
          metadata_,
          span.localRowBegin,
          span.rowCount,
          enqueue,
          enqueueCompressed);
    }
    return;
  }
  // Variable-size payloads cannot be planned until their parent offsets have
  // been decoded, but their offset/index buffers still can. Schedule these
  // first-stage ranges with other columns and let nested decoding issue the
  // second-stage payload plan after offsets are known.
  const auto& column = metadata_.column(physicalIndex);
  if (type->kind() == TypeKind::ROW && column.pages_size() > 0) {
    const auto firstDataPage = std::find_if(
        column.pages().begin(), column.pages().end(), [](const auto& page) {
          return page.length() > 0;
        });
    if (firstDataPage == column.pages().end()) {
      return;
    }
    const auto firstPageIndex =
        static_cast<int32_t>(firstDataPage - column.pages().begin());
    const auto& firstEncoding =
        metadata_.pageEncoding(physicalIndex, firstPageIndex);
    if (firstEncoding.array_encoding_case() == ArrayEncoding::kStruct) {
      // Nested list and map descendants need later payload planning, but their
      // first-stage offset ranges can be loaded with the rest of the batch.
      uint32_t childPhysicalIndex = physicalIndex + 1;
      for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
        enqueuePhysicalColumn(
            type->childAt(childIndex),
            childPhysicalIndex,
            rowStart,
            rowCount,
            {},
            structuralRequests);
        childPhysicalIndex += metadata_.physicalColumnSpan(childPhysicalIndex);
      }
      return;
    }
  }

  auto enqueue = [this](uint64_t offset, uint64_t length) {
    scheduleRead(offset, length);
  };
  NativeLanceColumnCursor cursor(
      physicalIndex, metadata_.pageRowStarts(physicalIndex));
  for (const auto& span : cursor.spans(rowStart, rowCount)) {
    const auto& page = column.pages(span.pageIndex);
    const auto& encoding =
        metadata_.pageEncoding(physicalIndex, span.pageIndex);
    enqueueEncodingRanges(
        encoding,
        column,
        page,
        metadata_,
        span.localRowBegin,
        span.rowCount,
        enqueue,
        enqueueCompressed);
  }
}

void NativeLancePageSource::scheduleStructuralPayloads(
    const std::vector<StructuralPageRangeRequest>& requests) const {
  if (input_.supportSyncLoad()) {
    return;
  }
  for (const auto& request : requests) {
    const auto plan = getOrCreateStructuralPagePlan(request);
    for (const auto& [offset, length] :
         plan->payloadRanges(request.localRowStart, request.rowCount)) {
      scheduleRead(offset, length);
    }
  }
}

std::shared_ptr<const NativeLanceStructuralPagePlan>
NativeLancePageSource::getOrCreateStructuralPagePlan(
    const StructuralPageRangeRequest& request) const {
  const NativeLancePageKey key{request.physicalColumnIndex, request.pageIndex};
  if (auto plan = findStructuralPagePlan(key)) {
    return plan;
  }
  const auto& page =
      metadata_.column(request.physicalColumnIndex).pages(request.pageIndex);
  const auto& layout =
      metadata_.pageLayout(request.physicalColumnIndex, request.pageIndex);
  auto candidate = prepareLanceStructuralPagePlan(
      page, layout, [this](uint64_t offset, uint64_t length) {
        return read(offset, length);
      });
  std::lock_guard<std::mutex> lock(structuralPagePlansMutex_);
  auto& plans = structuralPagePlans_.at(key.physicalColumn);
  const auto [it, inserted] = plans.emplace(key.pageIndex, candidate);
  return inserted ? std::move(candidate) : it->second;
}

std::shared_ptr<const NativeLanceStructuralPagePlan>
NativeLancePageSource::findStructuralPagePlan(NativeLancePageKey key) const {
  std::lock_guard<std::mutex> lock(structuralPagePlansMutex_);
  const auto& plans = structuralPagePlans_.at(key.physicalColumn);
  const auto it = plans.find(key.pageIndex);
  return it == plans.end() ? nullptr : it->second;
}

void NativeLancePageSource::releaseStructuralPagePlansBefore(
    NativeLancePageKey key) const {
  std::lock_guard<std::mutex> lock(structuralPagePlansMutex_);
  auto& plans = structuralPagePlans_.at(key.physicalColumn);
  plans.erase(plans.begin(), plans.lower_bound(key.pageIndex));
}

void NativeLancePageSource::releaseStructuralPagePlan(
    NativeLancePageKey key) const {
  std::lock_guard<std::mutex> lock(structuralPagePlansMutex_);
  structuralPagePlans_.at(key.physicalColumn).erase(key.pageIndex);
}

void NativeLancePageSource::scheduleRead(uint64_t offset, uint64_t length)
    const {
  readScheduler_.schedule(input_, offset, length);
}

void NativeLancePageSource::scheduleCompressedRead(
    std::string_view scheme,
    uint64_t offset,
    uint64_t length) const {
  BOLT_CHECK(!scheme.empty());
  if (scheme == "zstd") {
    std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
    if (legacyPageReaders_.find({offset, length}) != legacyPageReaders_.end()) {
      return;
    }
  }
  scheduleRead(offset, length);
}

void NativeLancePageSource::submitReadPlan() const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  readScheduler_.submit(input_);
}

void NativeLancePageSource::materializeReadPlan() const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  readScheduler_.materialize();
}

BufferPtr NativeLancePageSource::read(uint64_t offset, uint64_t length) const {
  std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
  BOLT_CHECK_LE(offset, input_.getReadFile()->size());
  BOLT_CHECK_LE(length, input_.getReadFile()->size() - offset);
  BOLT_CHECK_LE(
      length,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "Lance data range is too large: {} bytes",
      length);
  if (auto buffer = readScheduler_.take(offset, length)) {
    return buffer;
  }
  auto buffer = AlignedBuffer::allocate<char>(length, &pool_);
  auto stream = input_.enqueue({offset, length});
  input_.load(dwio::common::LogType::BLOCK);
  stream->readFully(buffer->asMutable<char>(), length);
  return buffer;
}

BufferPtr NativeLancePageSource::readCompressedRange(
    std::string_view scheme,
    uint64_t compressedOffset,
    uint64_t compressedLength,
    uint64_t decodedOffset,
    uint64_t decodedLength) const {
  const CompressedBufferKey key{compressedOffset, compressedLength};
  std::shared_ptr<NativeLanceLegacyPageReader> pageReader;
  {
    std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
    const auto session = legacyPageReaders_.find(key);
    if (session != legacyPageReaders_.end()) {
      pageReader = session->second;
    }
  }
  if (pageReader == nullptr) {
    auto candidate = std::make_shared<NativeLanceLegacyPageReader>(
        std::string(scheme),
        compressedOffset,
        compressedLength,
        pool_,
        [this](uint64_t offset, uint64_t length) {
          return read(offset, length);
        },
        compressedStreamChunkBytes_);
    std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
    const auto [session, inserted] = legacyPageReaders_.emplace(key, candidate);
    pageReader = inserted ? std::move(candidate) : session->second;
  }
  BOLT_CHECK(
      pageReader->accessMode() == NativeLanceDecompressor::accessMode(scheme));
  auto result = pageReader->decodeRange(decodedOffset, decodedLength);
  if (pageReader->finished()) {
    std::lock_guard<std::mutex> lock(legacyPageReadersMutex_);
    const auto session = legacyPageReaders_.find(key);
    if (session != legacyPageReaders_.end() &&
        session->second.get() == pageReader.get()) {
      legacyPageReaders_.erase(session);
    }
  }
  return result;
}

bool NativeLancePageSource::hasCompressedColumn(
    uint32_t columnIndex,
    uint64_t rowStart,
    uint64_t rowCount) const {
  BOLT_CHECK_LT(columnIndex, metadata_.rowType()->size());
  const auto firstPhysical = metadata_.physicalColumnIndex(columnIndex);
  const auto physicalEnd =
      firstPhysical + metadata_.physicalColumnSpan(firstPhysical);
  if (metadata_.usesStructuralEncoding()) {
    for (uint32_t physicalIndex = firstPhysical; physicalIndex < physicalEnd;
         ++physicalIndex) {
      NativeLanceColumnCursor cursor(
          physicalIndex, metadata_.pageRowStarts(physicalIndex));
      for (const auto& span : cursor.spans(rowStart, rowCount)) {
        if (lanceStructuralLayoutHasCompression(
                metadata_.pageLayout(physicalIndex, span.pageIndex))) {
          return true;
        }
      }
    }
    return false;
  }
  for (uint32_t physicalIndex = firstPhysical; physicalIndex < physicalEnd;
       ++physicalIndex) {
    NativeLanceColumnCursor cursor(
        physicalIndex, metadata_.pageRowStarts(physicalIndex));
    for (const auto& span : cursor.spans(rowStart, rowCount)) {
      if (hasCompressedFlatBuffer(
              metadata_.pageEncoding(physicalIndex, span.pageIndex))) {
        return true;
      }
    }
  }
  return false;
}

VectorPtr NativeLancePageSource::decodePhysicalColumn(
    const TypePtr& type,
    std::string_view logicalType,
    uint32_t physicalIndex,
    uint64_t rowStart,
    uint64_t rowCount,
    const std::vector<uint32_t>& arrayDimensions) const {
  BOLT_CHECK_LT(physicalIndex, metadata_.numPhysicalColumns());
  BOLT_CHECK_LE(rowStart, std::numeric_limits<uint64_t>::max() - rowCount);
  if (rowCount == 0) {
    return BaseVector::create(type, 0, &pool_);
  }
  const DecodeInput readInput(
      [this](uint64_t offset, uint64_t length) { return read(offset, length); },
      [this](
          std::string_view scheme,
          uint64_t compressedOffset,
          uint64_t compressedLength,
          uint64_t decodedOffset,
          uint64_t decodedLength) {
        return readCompressedRange(
            scheme,
            compressedOffset,
            compressedLength,
            decodedOffset,
            decodedLength);
      });
  const NativeLancePrefetchPhysicalColumn prefetchChild =
      [this](
          const TypePtr& childType,
          uint32_t childColumn,
          uint64_t childStart,
          uint64_t childCount) {
        prefetchPhysicalColumn(childType, childColumn, childStart, childCount);
      };
  const NativeLanceDecodePhysicalColumn decodeChild =
      [this](
          const TypePtr& childType,
          std::string_view childLogicalType,
          uint32_t childColumn,
          uint64_t childStart,
          uint64_t childCount) {
        return decodePhysicalColumn(
            childType, childLogicalType, childColumn, childStart, childCount);
      };
  if (metadata_.usesStructuralEncoding()) {
    const auto& column = metadata_.column(physicalIndex);
    uint64_t rowScale = 1;
    for (const auto dimension : arrayDimensions) {
      if (dimension == 0) {
        continue;
      }
      BOLT_CHECK_LE(rowScale, std::numeric_limits<uint64_t>::max() / dimension);
      rowScale *= dimension;
    }
    BOLT_CHECK_LE(rowStart, std::numeric_limits<uint64_t>::max() / rowScale);
    BOLT_CHECK_LE(rowCount, std::numeric_limits<uint64_t>::max() / rowScale);
    const auto physicalRowStart = rowStart * rowScale;
    const auto physicalRowCount = rowCount * rowScale;
    uint64_t outputOffset = 0;
    VectorPtr result;
    NativeLanceColumnCursor cursor(
        physicalIndex, metadata_.pageRowStarts(physicalIndex));
    const auto spans = cursor.spans(physicalRowStart, physicalRowCount);
    for (const auto& span : spans) {
      BOLT_CHECK_EQ(span.localRowBegin % rowScale, 0);
      BOLT_CHECK_EQ(span.rowCount % rowScale, 0);
      const auto localStart = span.localRowBegin / rowScale;
      const auto localCount = span.rowCount / rowScale;
      const auto& page = column.pages(span.pageIndex);
      const auto rangeRead = lanceStructuralPageSupportsRangeRead(
          type,
          arrayDimensions,
          metadata_.physicalColumnChildLogicalTypes(physicalIndex),
          metadata_.pageLayout(physicalIndex, span.pageIndex));
      const NativeLancePageKey pageKey{physicalIndex, span.pageIndex};
      releaseStructuralPagePlansBefore(pageKey);
      auto plan = rangeRead &&
              metadata_.pageLayout(physicalIndex, span.pageIndex)
                      .layout_case() ==
                  ::lance::encodings21::PageLayout::kMiniBlockLayout
          ? findStructuralPagePlan(pageKey)
          : std::shared_ptr<const NativeLanceStructuralPagePlan>{};
      if (rangeRead && plan == nullptr &&
          metadata_.pageLayout(physicalIndex, span.pageIndex).layout_case() ==
              ::lance::encodings21::PageLayout::kMiniBlockLayout) {
        plan = getOrCreateStructuralPagePlan(
            {.physicalColumnIndex = physicalIndex,
             .pageIndex = span.pageIndex,
             .localRowStart = span.localRowBegin,
             .rowCount = span.rowCount});
      }
      NativeLanceStructuralPageReader pageReader(
          {.key = pageKey,
           .type = type,
           .logicalType = metadata_.physicalColumnLogicalType(physicalIndex),
           .fixedSizeDimensions = arrayDimensions,
           .packedChildLogicalTypes =
               metadata_.physicalColumnChildLogicalTypes(physicalIndex),
           .localRowStart = localStart,
           .rowCount = localCount},
          metadata_,
          pool_,
          blobResolver_,
          input_.getName(),
          [this](const std::vector<std::pair<uint64_t, uint64_t>>& ranges) {
            std::lock_guard<std::recursive_mutex> guard(readPlanMutex_);
            readScheduler_.clearStage();
            for (const auto& [offset, length] : ranges) {
              scheduleRead(offset, length);
            }
            submitReadPlan();
          },
          readInput,
          std::move(plan));
      pageReader.decode();
      auto decoded = pageReader.consume();
      if (rangeRead && spans.size() == 1 && type->kind() != TypeKind::ARRAY &&
          type->kind() != TypeKind::MAP && type->kind() != TypeKind::ROW) {
        BOLT_CHECK_EQ(localCount, rowCount);
        BOLT_CHECK_EQ(decoded->size(), rowCount);
        if (span.localRowBegin + span.rowCount == page.length()) {
          releaseStructuralPagePlan(pageKey);
        }
        return decoded;
      }
      if (result == nullptr) {
        result = BaseVector::create(
            type, static_cast<vector_size_t>(rowCount), &pool_);
      }
      result->copy(
          decoded.get(),
          static_cast<vector_size_t>(outputOffset),
          static_cast<vector_size_t>(rangeRead ? 0 : localStart),
          static_cast<vector_size_t>(localCount));
      outputOffset += localCount;
      if (span.localRowBegin + span.rowCount == page.length()) {
        releaseStructuralPagePlan(pageKey);
      }
    }
    BOLT_CHECK_EQ(outputOffset, rowCount);
    BOLT_CHECK_NOT_NULL(result);
    return result;
  }
  if (metadata_.isBlobColumn(physicalIndex)) {
    return decodeLegacyBlobColumn(
        type,
        logicalType,
        physicalIndex,
        rowStart,
        rowCount,
        metadata_,
        pool_,
        input_,
        readInput.rawRead());
  }
  if (type->kind() == TypeKind::ROW) {
    const auto& header = metadata_.column(physicalIndex);
    const auto firstDataPage = std::find_if(
        header.pages().begin(), header.pages().end(), [](const auto& page) {
          return page.length() > 0;
        });
    if (firstDataPage != header.pages().end()) {
      BOLT_CHECK(firstDataPage->has_encoding());
      const auto firstPageIndex =
          static_cast<int32_t>(firstDataPage - header.pages().begin());
      const auto& firstEncoding =
          metadata_.pageEncoding(physicalIndex, firstPageIndex);
      if (firstEncoding.array_encoding_case() == ArrayEncoding::kPackedStruct) {
        BOLT_CHECK_LE(rowStart, metadata_.numRows());
        BOLT_CHECK_LE(rowCount, metadata_.numRows() - rowStart);
        uint64_t pageRowStart = 0;
        uint64_t outputOffset = 0;
        auto result = BaseVector::create(
            type, static_cast<vector_size_t>(rowCount), &pool_);
        for (int32_t pageIndex = 0; pageIndex < header.pages_size();
             ++pageIndex) {
          const auto& page = header.pages(pageIndex);
          if (page.length() == 0) {
            continue;
          }
          const auto pageRowEnd = pageRowStart + page.length();
          const auto overlapStart = std::max(rowStart, pageRowStart);
          const auto overlapEnd = std::min(rowStart + rowCount, pageRowEnd);
          if (overlapStart < overlapEnd) {
            const auto& encoding =
                metadata_.pageEncoding(physicalIndex, pageIndex);
            auto pageResult = decodeLegacyPackedStructPage(
                type,
                encoding,
                header,
                page,
                metadata_,
                physicalIndex,
                overlapStart - pageRowStart,
                overlapEnd - overlapStart,
                pool_,
                readInput.rawRead());
            if (outputOffset == 0 && pageResult->size() == rowCount) {
              return pageResult;
            }
            result->copy(
                pageResult.get(),
                static_cast<vector_size_t>(outputOffset),
                0,
                pageResult->size());
            outputOffset += pageResult->size();
          }
          pageRowStart = pageRowEnd;
        }
        BOLT_CHECK_EQ(outputOffset, rowCount);
        return result;
      }
    }
    uint64_t headerRows = 0;
    for (int32_t pageIndex = 0; pageIndex < header.pages_size(); ++pageIndex) {
      const auto& page = header.pages(pageIndex);
      if (page.length() == 0) {
        continue;
      }
      BOLT_CHECK(page.has_encoding(), "Lance struct page has no encoding");
      const auto& encoding = metadata_.pageEncoding(physicalIndex, pageIndex);
      BOLT_CHECK_EQ(
          encoding.array_encoding_case(),
          ArrayEncoding::kStruct,
          "Native Lance reader requires SimpleStruct for struct columns");
      BOLT_CHECK_LE(
          page.length(), std::numeric_limits<uint64_t>::max() - headerRows);
      headerRows += page.length();
    }
    BOLT_CHECK_LE(rowStart, headerRows);
    BOLT_CHECK_LE(rowCount, headerRows - rowStart);

    std::vector<VectorPtr> children;
    children.reserve(type->size());
    const auto scheduleChildrenSeparately = requiresDeferredRead(type);
    uint32_t childPhysicalIndex = physicalIndex + 1;
    std::vector<std::pair<TypePtr, uint32_t>> childrenToSchedule;
    for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
      if (scheduleChildrenSeparately) {
        childrenToSchedule.push_back(
            {type->childAt(childIndex), childPhysicalIndex});
      }
      childPhysicalIndex += metadata_.physicalColumnSpan(childPhysicalIndex);
    }
    if (!childrenToSchedule.empty()) {
      prefetchPhysicalColumns(childrenToSchedule, rowStart, rowCount);
    }
    childPhysicalIndex = physicalIndex + 1;
    for (uint32_t childIndex = 0; childIndex < type->size(); ++childIndex) {
      children.push_back(decodePhysicalColumn(
          type->childAt(childIndex),
          metadata_.physicalColumnLogicalType(childPhysicalIndex),
          childPhysicalIndex,
          rowStart,
          rowCount));
      childPhysicalIndex += metadata_.physicalColumnSpan(childPhysicalIndex);
    }
    BOLT_CHECK_EQ(
        childPhysicalIndex,
        physicalIndex + metadata_.physicalColumnSpan(physicalIndex));
    return std::make_shared<RowVector>(
        &pool_,
        type,
        nullptr,
        static_cast<vector_size_t>(rowCount),
        std::move(children));
  }
  auto result =
      BaseVector::create(type, static_cast<vector_size_t>(rowCount), &pool_);
  if (rowCount == 0) {
    return result;
  }

  const auto& column = metadata_.column(physicalIndex);
  uint64_t pageRowStart = 0;
  uint64_t outputOffset = 0;
  uint64_t itemsOffset = 0;
  const auto rowEnd = rowStart + rowCount;
  for (int32_t pageIndex = 0; pageIndex < column.pages_size(); ++pageIndex) {
    const auto& page = column.pages(pageIndex);
    if (page.length() == 0) {
      continue;
    }
    const auto pageRowEnd = pageRowStart + page.length();
    const auto overlapStart = std::max(rowStart, pageRowStart);
    const auto overlapEnd = std::min(rowEnd, pageRowEnd);
    if (overlapStart >= overlapEnd) {
      BOLT_CHECK(page.has_encoding(), "Lance page has no encoding");
      const auto& skippedEncoding =
          metadata_.pageEncoding(physicalIndex, pageIndex);
      if (skippedEncoding.array_encoding_case() == ArrayEncoding::kList) {
        itemsOffset += skippedEncoding.list().num_items();
      }
      pageRowStart = pageRowEnd;
      continue;
    }

    BOLT_CHECK(page.has_encoding(), "Lance page has no encoding");
    const auto& encoding = metadata_.pageEncoding(physicalIndex, pageIndex);
    const auto localStart = overlapStart - pageRowStart;
    const auto localCount = overlapEnd - overlapStart;
    const auto* unwrappedEncoding = &encoding;
    if (encoding.array_encoding_case() == ArrayEncoding::kNullable) {
      const auto& nullable = encoding.nullable();
      if (nullable.nullability_case() ==
          ::lance::encodings::Nullable::kNoNulls) {
        unwrappedEncoding = &nullable.no_nulls().values();
      } else if (
          nullable.nullability_case() ==
          ::lance::encodings::Nullable::kSomeNulls) {
        unwrappedEncoding = &nullable.some_nulls().values();
      }
    }
    if (unwrappedEncoding->array_encoding_case() ==
        ArrayEncoding::kFixedSizeList) {
      auto pageResult = decodeLegacyFixedSizeListPage(
          type,
          logicalType,
          encoding,
          column,
          page,
          metadata_,
          localStart,
          localCount,
          pool_,
          readInput.rawRead(),
          readInput.compressedRead());
      if (outputOffset == 0 && pageResult->size() == rowCount) {
        return pageResult;
      }
      result->copy(
          pageResult.get(),
          static_cast<vector_size_t>(outputOffset),
          0,
          static_cast<vector_size_t>(localCount));
      outputOffset += localCount;
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kDictionary) {
      auto pageResult = decodeLegacyDictionaryPage(
          type,
          logicalType,
          encoding,
          column,
          page,
          metadata_,
          localStart,
          localCount,
          pool_,
          readInput.rawRead(),
          readInput.compressedRead());
      if (outputOffset == 0 && localCount == rowCount) {
        result = std::move(pageResult);
      } else {
        result->copy(
            pageResult.get(),
            static_cast<vector_size_t>(outputOffset),
            0,
            static_cast<vector_size_t>(localCount));
      }
      outputOffset += localCount;
      pageRowStart = pageRowEnd;
      continue;
    }
    if ((type->kind() == TypeKind::VARCHAR ||
         type->kind() == TypeKind::VARBINARY) &&
        isLegacyBinaryEncoding(encoding)) {
      auto pageResult = decodeLegacyBinaryPage(
          type,
          encoding,
          column,
          page,
          metadata_,
          localStart,
          localCount,
          pool_,
          readInput.rawRead(),
          readInput.compressedRead());
      if (outputOffset == 0 && localCount == rowCount) {
        result = std::move(pageResult);
      } else {
        result->copy(
            pageResult.get(),
            static_cast<vector_size_t>(outputOffset),
            0,
            static_cast<vector_size_t>(localCount));
      }
      outputOffset += localCount;
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kList &&
        (type->kind() == TypeKind::VARCHAR ||
         type->kind() == TypeKind::VARBINARY)) {
      auto pageResult = decodeLegacyListPage(
          type,
          encoding,
          physicalIndex,
          column,
          page,
          metadata_,
          localStart,
          localCount,
          itemsOffset,
          pool_,
          readInput.rawRead(),
          readInput.compressedRead(),
          prefetchChild,
          decodeChild);
      result->copy(
          pageResult.get(),
          static_cast<vector_size_t>(outputOffset),
          0,
          static_cast<vector_size_t>(localCount));
      outputOffset += localCount;
      itemsOffset += encoding.list().num_items();
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kList &&
        type->kind() == TypeKind::ARRAY) {
      auto pageResult = decodeLegacyListPage(
          type,
          encoding,
          physicalIndex,
          column,
          page,
          metadata_,
          localStart,
          localCount,
          itemsOffset,
          pool_,
          readInput.rawRead(),
          readInput.compressedRead(),
          prefetchChild,
          decodeChild);
      if (outputOffset == 0 && pageResult->size() == rowCount) {
        return pageResult;
      }
      result->copy(
          pageResult.get(),
          static_cast<vector_size_t>(outputOffset),
          0,
          static_cast<vector_size_t>(localCount));
      outputOffset += localCount;
      itemsOffset += encoding.list().num_items();
      pageRowStart = pageRowEnd;
      continue;
    }
    if (encoding.array_encoding_case() == ArrayEncoding::kList &&
        type->kind() == TypeKind::MAP) {
      auto pageResult = decodeLegacyListPage(
          type,
          encoding,
          physicalIndex,
          column,
          page,
          metadata_,
          localStart,
          localCount,
          itemsOffset,
          pool_,
          readInput.rawRead(),
          readInput.compressedRead(),
          prefetchChild,
          decodeChild);
      if (outputOffset == 0 && pageResult->size() == rowCount) {
        return pageResult;
      }
      result->copy(
          pageResult.get(),
          static_cast<vector_size_t>(outputOffset),
          0,
          static_cast<vector_size_t>(localCount));
      outputOffset += localCount;
      itemsOffset += encoding.list().num_items();
      pageRowStart = pageRowEnd;
      continue;
    }
    auto pageResult = decodeLegacyPrimitivePage(
        type,
        logicalType,
        encoding,
        column,
        page,
        metadata_,
        localStart,
        localCount,
        pool_,
        readInput.rawRead(),
        readInput.compressedRead());
    if (outputOffset == 0 && pageResult->size() == rowCount) {
      return pageResult;
    }
    result->copy(
        pageResult.get(),
        static_cast<vector_size_t>(outputOffset),
        0,
        static_cast<vector_size_t>(localCount));
    outputOffset += localCount;
    pageRowStart = pageRowEnd;
  }

  BOLT_CHECK_EQ(
      outputOffset,
      rowCount,
      "Lance column {} contains fewer rows than requested",
      physicalIndex);
  return result;
}

} // namespace bytedance::bolt::lance::reader
