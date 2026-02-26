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

#include <lz4.h>
#include <thrift/protocol/TCompactProtocol.h> // @manual
#include <zstd.h>
#include <zstd_errors.h>

#include "bolt/dwio/common/BufferUtil.h"
#include "bolt/dwio/common/ColumnVisitors.h"
#include "bolt/dwio/parquet/reader/Decompression.h"
#include "bolt/dwio/parquet/reader/NestedStructureDecoder.h"
#include "bolt/dwio/parquet/reader/PageReader.h"
#include "bolt/dwio/parquet/thrift/FmtParquetFormatters.h"
#include "bolt/dwio/parquet/thrift/ThriftTransport.h"
#include "bolt/vector/FlatVector.h"

#include "bolt/dwio/parquet/thrift/ThriftInternal.h"

namespace bytedance::bolt::parquet {

using thrift::Encoding;
using thrift::PageHeader;

void PageReader::seekToPage(int64_t row, bool keepRepDefRawData) {
  defineDecoder_.reset();
  repeatDecoder_.reset();
  // 'rowOfPage_' is the row number of the first row of the next page.
  rowOfPage_ += numRowsInPage_;
  for (;;) {
    auto dataStart = pageStart_;
    if (chunkSize_ <= pageStart_) {
      // This may happen if seeking to exactly end of row group.
      numRepDefsInPage_ = 0;
      numRowsInPage_ = 0;
      break;
    }
    PageHeader pageHeader = readPageHeader();
    pageStart_ = pageDataStart_ + pageHeader.compressed_page_size;

    if (cryptoCtx_.dataDecryptor != nullptr) {
      updateDecryption(
          cryptoCtx_.dataDecryptor, encryption::kDictionaryPage, &dataPageAad_);
    }

    switch (pageHeader.type) {
      case thrift::PageType::DATA_PAGE:
        prepareDataPageV1(pageHeader, row, keepRepDefRawData);
        break;
      case thrift::PageType::DATA_PAGE_V2:
        prepareDataPageV2(pageHeader, row, keepRepDefRawData);
        break;
      case thrift::PageType::DICTIONARY_PAGE:
        if (row == kRepDefOnly) {
          skipBytes(
              pageHeader.compressed_page_size,
              inputStream_.get(),
              bufferStart_,
              bufferEnd_);
          cryptoCtx_.startDecryptWithDictionaryPage = false;
          continue;
        }
        prepareDictionary(pageHeader);
        continue;
      default:
        break; // ignore INDEX page type and any other custom extensions
    }
    if (row == kRepDefOnly || row < rowOfPage_ + numRowsInPage_) {
      break;
    }
    updateRowInfoAfterPageSkipped();
  }
}

PageHeader PageReader::readPageHeader() {
  if (bufferEnd_ == bufferStart_) {
    const void* buffer;
    int32_t size;
    inputStream_->Next(&buffer, &size);
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + size;
  }

  PageHeader pageHeader;
  uint64_t readBytes;
  if (cryptoCtx_.metaDecryptor != nullptr) {
    thrift::ThriftDeserializer deserializer;
    updateDecryption(
        cryptoCtx_.metaDecryptor,
        encryption::kDictionaryPageHeader,
        &dataPageHeaderAad_);
    uint32_t encryptedBufferSize = bufferEnd_ - bufferStart_;
    uint8_t* encryptedBufferStart =
        const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(bufferStart_));
    uint32_t headerSize = encryptedBufferSize;
    bool reallocate = false;
    auto decryptBufferGuard = folly::makeGuard([&]() {
      if (reallocate) {
        pool_.free(encryptedBufferStart, encryptedBufferSize);
      }
    });

    while (true) {
      headerSize = encryptedBufferSize;
      if (headerSize > cryptoCtx_.metaDecryptor->CiphertextSizeDelta()) {
        try {
          bool ret = deserializer.DeserializeMessage(
              encryptedBufferStart,
              &headerSize,
              &pageHeader,
              cryptoCtx_.metaDecryptor);
          if (ret) {
            break;
          }
        } catch (const BoltRuntimeError& e) {
          BOLT_CHECK(
              encryptedBufferSize <= kDefaultMaxPageHeaderSize,
              "Failed to decrypt page header with page header more than 16MB, failed reason: {}",
              e.message());
        }
      }

      // inputStream_->Next may change the content in buffer, so do redundant
      // allocate and copy from buffer when first decrypt failed before reading
      // from inputstream
      if (!reallocate) {
        encryptedBufferStart =
            reinterpret_cast<uint8_t*>(pool_.allocate(encryptedBufferSize));
        memcpy(encryptedBufferStart, bufferStart_, encryptedBufferSize);
      }

      reallocate = true;
      int32_t size;
      if (!inputStream_->Next(
              reinterpret_cast<const void**>(&bufferStart_), &size)) {
        BOLT_FAIL("readPageHeader() reading past the end of the stream");
      }
      bufferEnd_ = bufferStart_ + size;

      encryptedBufferStart = reinterpret_cast<uint8_t*>(pool_.reallocate(
          encryptedBufferStart,
          encryptedBufferSize,
          encryptedBufferSize + size));

      memcpy(encryptedBufferStart + encryptedBufferSize, bufferStart_, size);
      encryptedBufferSize += size;
    }
    readBytes = headerSize;
    bufferStart_ = bufferEnd_ - (encryptedBufferSize - headerSize);
  } else {
    std::shared_ptr<thrift::ThriftTransport> transport =
        std::make_shared<thrift::ThriftStreamingTransport>(
            inputStream_.get(), bufferStart_, bufferEnd_);
    apache::thrift::protocol::TCompactProtocolT<thrift::ThriftTransport>
        protocol(transport);
    readBytes = pageHeader.read(&protocol);
  }

  pageDataStart_ = pageStart_ + readBytes;
  return pageHeader;
}

const char* PageReader::readBytes(int32_t size, BufferPtr& copy) {
  if (bufferEnd_ == bufferStart_) {
    const void* buffer = nullptr;
    int32_t bufferSize = 0;
    if (!inputStream_->Next(&buffer, &bufferSize)) {
      BOLT_FAIL("Read past end");
    }
    bufferStart_ = reinterpret_cast<const char*>(buffer);
    bufferEnd_ = bufferStart_ + bufferSize;
  }
  if (bufferEnd_ - bufferStart_ >= size) {
    bufferStart_ += size;
    return bufferStart_ - size;
  }
  dwio::common::ensureCapacity<char>(copy, size, &pool_);
  dwio::common::readBytes(
      size,
      inputStream_.get(),
      copy->asMutable<char>(),
      bufferStart_,
      bufferEnd_);
  return copy->as<char>();
}

const char* FOLLY_NONNULL PageReader::decompressData(
    const char* pageData,
    uint32_t compressedSize,
    uint32_t uncompressedSize) {
  NanosecondTimer timer(
      FOLLY_LIKELY(statis_ != nullptr) ? &statis_->decompressDataTimeNs
                                       : nullptr);

  dwio::common::ensureCapacity<char>(
      decompressedData_, uncompressedSize, &pool_);
  if (codec_ == thrift::CompressionCodec::LZ4 ||
      codec_ == thrift::CompressionCodec::LZO) {
    return decompressLz4AndLzo(
        pageData,
        decompressedData_,
        compressedSize,
        uncompressedSize,
        pool_,
        codec_);
  } else if (codec_ == thrift::CompressionCodec::ZSTD) {
    return bdZstdDecompression(
        pageData,
        decompressedData_,
        compressedSize,
        uncompressedSize,
        pool_,
        codec_);
  } else {
    // This function utilizes DWIO's compressor factory, supporting all codec
    // types except BROTLI. It works for the following codecs:
    //  - GZIP: which uses the deflate stream. For more details, see:
    //    https://parquet.apache.org/docs/file-format/data-pages/compression/#gzip
    //  - SNAPPY
    //  - UNCOMPRESSED
    //  - ZSTD, LZ4/LZO/LZ4_RAW: handled differently prior to this point.
    // For BROTLI, the function will throw a BOLT_UNSUPPORTED exception.
    return bdCodecDecompression(
        pageData,
        decompressedData_,
        compressedSize,
        uncompressedSize,
        pool_,
        codec_);
  }
}

void PageReader::setPageRowInfo(bool forRepDef) {
  if (isTopLevel_ || forRepDef || maxRepeat_ == 0) {
    numRowsInPage_ = numRepDefsInPage_;
  } else if (hasChunkRepDefs_) {
    ++pageIndex_;
    BOLT_CHECK_LT(
        pageIndex_,
        numLeavesInPage_.size(),

        "Seeking past known repdefs for non top level column page {}",
        pageIndex_);
    numRowsInPage_ = numLeavesInPage_[pageIndex_];
  } else {
    numRowsInPage_ = kRowsUnknown;
  }
}

void PageReader::readPageDefLevels() {
  BOLT_CHECK(kRowsUnknown == numRowsInPage_ || maxDefine_ > 1);
  definitionLevels_.resize(numRepDefsInPage_);
  BOLT_CHECK_NOT_NULL(
      wideDefineDecoder_, "parquet read error with maxDefine = {}", maxDefine_);
  wideDefineDecoder_->GetBatch(definitionLevels_.data(), numRepDefsInPage_);
  leafNulls_.resize(bits::nwords(numRepDefsInPage_));
  leafNullsSize_ = getLengthsAndNulls(
      LevelMode::kNulls,
      leafInfo_,
      0,
      numRepDefsInPage_,
      numRepDefsInPage_,
      nullptr,
      leafNulls_.data(),
      0);
  numRowsInPage_ = leafNullsSize_;
  numLeafNullsConsumed_ = 0;
}

void PageReader::updateRowInfoAfterPageSkipped() {
  rowOfPage_ += numRowsInPage_;
  if (hasChunkRepDefs_) {
    numLeafNullsConsumed_ = rowOfPage_;
  }
}

void PageReader::prepareDataPageV1(
    const PageHeader& pageHeader,
    int64_t row,
    const bool keepRepDefRawData) {
  BOLT_CHECK(
      pageHeader.type == thrift::PageType::DATA_PAGE &&
      pageHeader.__isset.data_page_header);
  ++pageOrdinal_;
  numRepDefsInPage_ = pageHeader.data_page_header.num_values;
  setPageRowInfo(row == kRepDefOnly);
  if (row != kRepDefOnly && numRowsInPage_ != kRowsUnknown &&
      numRowsInPage_ + rowOfPage_ <= row) {
    dwio::common::skipBytes(
        pageHeader.compressed_page_size,
        inputStream_.get(),
        bufferStart_,
        bufferEnd_);

    return;
  }

  int32_t compressedLen = pageHeader.compressed_page_size;
  pageData_ = readBytes(compressedLen, pageBuffer_);
  if (cryptoCtx_.dataDecryptor != nullptr) {
    decryptPageData(compressedLen);
  }
  pageData_ = decompressData(
      pageData_, compressedLen, pageHeader.uncompressed_page_size);
  auto pageEnd = pageData_ + pageHeader.uncompressed_page_size;

  // copy rep/def to preloadedRepDefs_, do not decode
  if (row == kRepDefOnly && keepRepDefRawData) {
    constexpr int32_t kLenSize = sizeof(int32_t);
    auto repDefStart = pageData_;
    auto startBytes = totalRefDefBytes_;
    if (maxRepeat_ > 0) {
      uint32_t repeatLength = readField<int32_t>(pageData_);
      pageData_ += repeatLength;
      totalRefDefBytes_ += repeatLength + kLenSize;
    }
    if (maxDefine_ > 0) {
      auto defineLength = readField<uint32_t>(pageData_);
      pageData_ += defineLength;
      totalRefDefBytes_ += defineLength + kLenSize;
    }
    auto repDefLen = totalRefDefBytes_ - startBytes;
    auto& refDefData = preloadedRepDefs_.back();
    auto offset = refDefData.size();
    // 4 more bytes for numRepDefsInPage_
    auto totalSize = offset + repDefLen + kLenSize;
    refDefData.resize(totalSize);
    BOLT_CHECK(repDefLen > 0);
    *(reinterpret_cast<int32_t*>(refDefData.data() + offset)) =
        numRepDefsInPage_;
    simd::memcpy(refDefData.data() + offset + kLenSize, repDefStart, repDefLen);
  } else {
    if (maxRepeat_ > 0) {
      uint32_t repeatLength = readField<int32_t>(pageData_);
      repeatDecoder_ = std::make_unique<::arrow::util::RleDecoder>(
          reinterpret_cast<const uint8_t*>(pageData_),
          repeatLength,
          ::arrow::bit_util::NumRequiredBits(maxRepeat_));

      pageData_ += repeatLength;
      totalRefDefBytes_ += repeatLength + sizeof(int32_t);
    }

    if (maxDefine_ > 0) {
      auto defineLength = readField<uint32_t>(pageData_);
      if (maxDefine_ == 1) {
        defineDecoder_ = std::make_unique<RleBpDecoder>(
            pageData_,
            pageData_ + defineLength,
            ::arrow::bit_util::NumRequiredBits(maxDefine_));
      }
      wideDefineDecoder_ = std::make_unique<::arrow::util::RleDecoder>(
          reinterpret_cast<const uint8_t*>(pageData_),
          defineLength,
          ::arrow::bit_util::NumRequiredBits(maxDefine_));
      pageData_ += defineLength;
      totalRefDefBytes_ += defineLength + sizeof(uint32_t);
    }
  }
  encodedDataSize_ = pageEnd - pageData_;

  encoding_ = pageHeader.data_page_header.encoding;
  if (!hasChunkRepDefs_ && (numRowsInPage_ == kRowsUnknown || maxDefine_ > 1)) {
    readPageDefLevels();
  }

  if (row != kRepDefOnly) {
    makeDecoder();
  }
}

void PageReader::prepareDataPageV2(
    const PageHeader& pageHeader,
    int64_t row,
    const bool keepRepDefRawData) {
  BOLT_CHECK(pageHeader.__isset.data_page_header_v2);
  ++pageOrdinal_;

  numRepDefsInPage_ = pageHeader.data_page_header_v2.num_values;
  setPageRowInfo(row == kRepDefOnly);
  if (row != kRepDefOnly && numRowsInPage_ != kRowsUnknown &&
      numRowsInPage_ + rowOfPage_ <= row) {
    skipBytes(
        pageHeader.compressed_page_size,
        inputStream_.get(),
        bufferStart_,
        bufferEnd_);
    return;
  }

  uint32_t defineLength = maxDefine_ > 0
      ? pageHeader.data_page_header_v2.definition_levels_byte_length
      : 0;
  uint32_t repeatLength = maxRepeat_ > 0
      ? pageHeader.data_page_header_v2.repetition_levels_byte_length
      : 0;
  auto bytes = pageHeader.compressed_page_size;
  pageData_ = readBytes(bytes, pageBuffer_);

  if (cryptoCtx_.dataDecryptor != nullptr) {
    decryptPageData(bytes);
  }

  if (row == kRepDefOnly && keepRepDefRawData) {
    BOLT_CHECK(defineLength > 0 && repeatLength > 0);
    constexpr int32_t kLenSize = sizeof(int32_t);
    auto& refDefData = preloadedRepDefs_.back();
    auto offset = refDefData.size();
    auto totalSize = offset + defineLength + repeatLength + 3 * kLenSize;
    refDefData.resize(totalSize);
    *(reinterpret_cast<int32_t*>(refDefData.data() + offset)) =
        numRepDefsInPage_;
    *(reinterpret_cast<int32_t*>(refDefData.data() + offset + kLenSize)) =
        repeatLength;
    simd::memcpy(
        refDefData.data() + offset + 2 * kLenSize, pageData_, repeatLength);
    *(reinterpret_cast<int32_t*>(
        refDefData.data() + offset + 2 * kLenSize + repeatLength)) =
        defineLength;
    simd::memcpy(
        refDefData.data() + offset + 3 * kLenSize + repeatLength,
        pageData_ + repeatLength,
        defineLength);
    // do not need to decompressData
    return;
  }

  if (repeatLength) {
    repeatDecoder_ = std::make_unique<::arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_),
        repeatLength,
        ::arrow::bit_util::NumRequiredBits(maxRepeat_));
  }

  if (maxDefine_ > 0) {
    defineDecoder_ = std::make_unique<RleBpDecoder>(
        pageData_ + repeatLength,
        pageData_ + repeatLength + defineLength,
        ::arrow::bit_util::NumRequiredBits(maxDefine_));
    wideDefineDecoder_ = std::make_unique<::arrow::util::RleDecoder>(
        reinterpret_cast<const uint8_t*>(pageData_ + repeatLength),
        defineLength,
        ::arrow::bit_util::NumRequiredBits(maxDefine_));
  }
  auto levelsSize = repeatLength + defineLength;
  pageData_ += levelsSize;
  if (pageHeader.data_page_header_v2.__isset.is_compressed ||
      pageHeader.data_page_header_v2.is_compressed) {
    pageData_ = decompressData(
        pageData_,
        pageHeader.compressed_page_size - levelsSize,
        pageHeader.uncompressed_page_size - levelsSize);
  }
  if (row == kRepDefOnly) {
    return;
  }

  encodedDataSize_ = pageHeader.uncompressed_page_size - levelsSize;
  encoding_ = pageHeader.data_page_header_v2.encoding;
  if (!hasChunkRepDefs_ && (numRowsInPage_ == kRowsUnknown || maxDefine_ > 1)) {
    readPageDefLevels();
  }
  if (row != kRepDefOnly) {
    makeDecoder();
  }
}

void PageReader::prepareDictionary(const PageHeader& pageHeader) {
  dictionary_.numValues = pageHeader.dictionary_page_header.num_values;
  dictionaryEncoding_ = pageHeader.dictionary_page_header.encoding;
  dictionary_.sorted = pageHeader.dictionary_page_header.__isset.is_sorted &&
      pageHeader.dictionary_page_header.is_sorted;
  BOLT_CHECK(
      dictionaryEncoding_ == Encoding::PLAIN_DICTIONARY ||
      dictionaryEncoding_ == Encoding::PLAIN);

  cryptoCtx_.startDecryptWithDictionaryPage = false;
  int32_t compressedLen = pageHeader.compressed_page_size;
  int32_t uncompressedLen = pageHeader.uncompressed_page_size;
  if (codec_ != thrift::CompressionCodec::UNCOMPRESSED) {
    pageData_ = readBytes(compressedLen, pageBuffer_);
    if (cryptoCtx_.dataDecryptor != nullptr) {
      decryptPageData(compressedLen);
    }
    pageData_ = decompressData(
        pageData_, compressedLen, pageHeader.uncompressed_page_size);
  } else {
    if (cryptoCtx_.dataDecryptor != nullptr) {
      pageData_ = readBytes(uncompressedLen, pageBuffer_);
      decryptPageData(uncompressedLen);
    }
  }

  auto parquetType = type_->parquetType_.value();
  switch (parquetType) {
    case thrift::Type::INT32:
    case thrift::Type::INT64:
    case thrift::Type::FLOAT:
    case thrift::Type::DOUBLE: {
      int32_t typeSize = (parquetType == thrift::Type::INT32 ||
                          parquetType == thrift::Type::FLOAT)
          ? sizeof(float)
          : sizeof(double);
      auto numBytes = dictionary_.numValues * typeSize;
      if (type_->type()->isShortDecimal() &&
          parquetType == thrift::Type::INT32) {
        auto boltTypeLength = type_->type()->cppSizeInBytes();
        auto numBoltBytes = dictionary_.numValues * boltTypeLength;
        dictionary_.values =
            AlignedBuffer::allocate<char>(numBoltBytes, &pool_);
      } else if (type_->type()->isTimestamp()) {
        const auto numBoltBytes = dictionary_.numValues * sizeof(int128_t);
        dictionary_.values =
            AlignedBuffer::allocate<char>(numBoltBytes, &pool_);
      } else {
        dictionary_.values = AlignedBuffer::allocate<char>(numBytes, &pool_);
      }
      if (pageData_) {
        memcpy(dictionary_.values->asMutable<char>(), pageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes,
            inputStream_.get(),
            dictionary_.values->asMutable<char>(),
            bufferStart_,
            bufferEnd_);
      }
      if (type_->type()->isShortDecimal() &&
          parquetType == thrift::Type::INT32) {
        auto values = dictionary_.values->asMutable<int64_t>();
        auto parquetValues = dictionary_.values->asMutable<int32_t>();
        for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
          // Expand the Parquet type length values to Bolt type length.
          // We start from the end to allow in-place expansion.
          values[i] = parquetValues[i];
        }
      } else if (type_->type()->isTimestamp()) {
        BOLT_DCHECK_EQ(parquetType, thrift::Type::INT64);
        auto values = dictionary_.values->asMutable<int128_t>();
        auto parquetValues = dictionary_.values->asMutable<int64_t>();
        for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
          // Expand the Parquet type length values to Bolt type length.
          // We start from the end to allow in-place expansion.
          values[i] = parquetValues[i];
        }
      }
      break;
    }
    case thrift::Type::INT96: {
      auto numBoltBytes = dictionary_.numValues * sizeof(Timestamp);
      dictionary_.values = AlignedBuffer::allocate<char>(numBoltBytes, &pool_);
      auto numBytes = dictionary_.numValues * sizeof(Int96Timestamp);
      if (pageData_) {
        memcpy(dictionary_.values->asMutable<char>(), pageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes,
            inputStream_.get(),
            dictionary_.values->asMutable<char>(),
            bufferStart_,
            bufferEnd_);
      }
      // Expand the Parquet type length values to Bolt type length.
      // We start from the end to allow in-place expansion.
      auto values = dictionary_.values->asMutable<Timestamp>();
      auto parquetValues = dictionary_.values->asMutable<char>();

      for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
        // Convert the timestamp into seconds and nanos since the Unix epoch,
        // 00:00:00.000000 on 1 January 1970.
        int64_t nanos;
        memcpy(
            &nanos,
            parquetValues + i * sizeof(Int96Timestamp),
            sizeof(int64_t));
        int32_t days;
        memcpy(
            &days,
            parquetValues + i * sizeof(Int96Timestamp) + sizeof(int64_t),
            sizeof(int32_t));
        values[i] = Timestamp::fromDaysAndNanos(days, nanos);
      }
      break;
    }
    case thrift::Type::BYTE_ARRAY: {
      dictionary_.values =
          AlignedBuffer::allocate<StringView>(dictionary_.numValues, &pool_);
      auto numBytes = uncompressedLen;
      auto values = dictionary_.values->asMutable<StringView>();
      dictionary_.strings = AlignedBuffer::allocate<char>(numBytes, &pool_);
      auto strings = dictionary_.strings->asMutable<char>();
      if (pageData_) {
        memcpy(strings, pageData_, numBytes);
      } else {
        dwio::common::readBytes(
            numBytes, inputStream_.get(), strings, bufferStart_, bufferEnd_);
      }
      auto header = strings;
      for (auto i = 0; i < dictionary_.numValues; ++i) {
        auto length = *reinterpret_cast<const int32_t*>(header);
        values[i] = StringView(header + sizeof(int32_t), length);
        header += length + sizeof(int32_t);
      }
      BOLT_CHECK_EQ(header, strings + numBytes);
      break;
    }
    case thrift::Type::FIXED_LEN_BYTE_ARRAY: {
      auto parquetTypeLength = type_->typeLength_;
      auto numParquetBytes = dictionary_.numValues * parquetTypeLength;
      auto boltTypeLength = type_->type()->cppSizeInBytes();
      auto numBoltBytes = dictionary_.numValues * boltTypeLength;
      dictionary_.values = AlignedBuffer::allocate<char>(numBoltBytes, &pool_);
      auto data = dictionary_.values->asMutable<char>();
      // Read the data bytes.
      if (pageData_) {
        memcpy(data, pageData_, numParquetBytes);
      } else {
        dwio::common::readBytes(
            numParquetBytes,
            inputStream_.get(),
            data,
            bufferStart_,
            bufferEnd_);
      }
      if (type_->type()->isShortDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
        if (numParquetBytes < numBoltBytes) {
          auto values = dictionary_.values->asMutable<int64_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Bolt type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int64_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + boltTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<int64_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = __builtin_bswap64(values[i]);
        }
        break;
      } else if (type_->type()->isLongDecimal()) {
        // Parquet decimal values have a fixed typeLength_ and are in big-endian
        // layout.
        if (numParquetBytes < numBoltBytes) {
          auto values = dictionary_.values->asMutable<int128_t>();
          for (auto i = dictionary_.numValues - 1; i >= 0; --i) {
            // Expand the Parquet type length values to Bolt type length.
            // We start from the end to allow in-place expansion.
            auto sourceValue = data + (i * parquetTypeLength);
            int128_t value = *sourceValue >= 0 ? 0 : -1;
            memcpy(
                reinterpret_cast<uint8_t*>(&value) + boltTypeLength -
                    parquetTypeLength,
                sourceValue,
                parquetTypeLength);
            values[i] = value;
          }
        }
        auto values = dictionary_.values->asMutable<int128_t>();
        for (auto i = 0; i < dictionary_.numValues; ++i) {
          values[i] = bits::builtin_bswap128(values[i]);
        }
        break;
      }
      BOLT_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
    }
    default:
      BOLT_UNSUPPORTED(
          "Parquet type {} not supported for dictionary", parquetType);
  }
}

void PageReader::makeFilterCache(dwio::common::ScanState& state) {
  BOLT_CHECK(!state.dictionary2.values, "Parquet supports only one dictionary");
  state.filterCache.resize(state.dictionary.numValues);
  simd::memset(
      state.filterCache.data(),
      dwio::common::FilterResult::kUnknown,
      state.filterCache.size());
  state.rawState.filterCache = state.filterCache.data();
}

namespace {
int32_t parquetTypeBytes(thrift::Type::type type) {
  switch (type) {
    case thrift::Type::INT32:
    case thrift::Type::FLOAT:
      return 4;
    case thrift::Type::INT64:
    case thrift::Type::DOUBLE:
      return 8;
    case thrift::Type::INT96:
      return 12;
    default:
      BOLT_FAIL("Type does not have a byte width {}", type);
  }
}
} // namespace

void PageReader::preloadPageRepDefs(const bool keepRepDefRawData) {
  seekToPage(kRepDefOnly, keepRepDefRawData);
  if (!keepRepDefRawData) {
    BOLT_CHECK_GE(
        (int64_t)INT_MAX,
        (int64_t)definitionLevels_.size() + numRepDefsInPage_,
        "Cannot reserve additional contiguous bytes in the vectorized reader (integer overflow). definitionLevels_ size:{}, numRepDefsInPage_:{}.",
        definitionLevels_.size(),
        numRepDefsInPage_);
    auto begin = definitionLevels_.size();
    auto numLevels = definitionLevels_.size() + numRepDefsInPage_;
    definitionLevels_.resize(numLevels);
    BOLT_CHECK_NOT_NULL(
        wideDefineDecoder_,
        "parquet read error with maxDefine = {}",
        maxDefine_);
    wideDefineDecoder_->GetBatch(
        definitionLevels_.data() + begin, numRepDefsInPage_);
    if (repeatDecoder_) {
      repetitionLevels_.resize(numLevels);

      repeatDecoder_->GetBatch(
          repetitionLevels_.data() + begin, numRepDefsInPage_);
    }
    leafNulls_.resize(bits::nwords(leafNullsSize_ + numRepDefsInPage_));
    auto numLeaves = getLengthsAndNulls(
        LevelMode::kNulls,
        leafInfo_,
        begin,
        begin + numRepDefsInPage_,
        numRepDefsInPage_,
        nullptr,
        leafNulls_.data(),
        leafNullsSize_);
    leafNullsSize_ += numLeaves;
    numLeavesInPage_.push_back(numLeaves);
  }
  return;
}

void PageReader::preloadRepDefs() {
  hasChunkRepDefs_ = true;
  bool startWithDictinoaryPage = cryptoCtx_.startDecryptWithDictionaryPage;
  if (maxRepeat_ > 0 && maxDefine_ > 0) {
    const int32_t samplePages = decodeRepDefPageCount_;
    int32_t pageCnt = 0;
    totalRefDefBytes_ = 0;
    // load rep/def fully for first 10 pages as sampling
    while (pageStart_ < chunkSize_ && ++pageCnt <= samplePages) {
      preloadPageRepDefs(false);
    }

    if (pageStart_ < chunkSize_) {
      BOLT_CHECK(definitionLevels_.size() > 0);
      // at least one page
      auto pageCntPerBatch = repDefMemoryLimit_ /
              ((definitionLevels_.size() * sizeof(int16_t)) / samplePages) +
          1;
      auto avgRefDefBytes = totalRefDefBytes_ / samplePages + 1;
      auto avgPageLen = pageStart_ / samplePages + 1;
      LOG(INFO) << __FUNCTION__ << "[" << this
                << "] : def/rep levels = " << definitionLevels_.size()
                << ", avgPageLen = " << avgPageLen
                << ", chunkSize = " << chunkSize_
                << ", pagesCntPerBatch = " << pageCntPerBatch
                << ", samplePages = " << samplePages << ", repDefMemoryLimit_ "
                << repDefMemoryLimit_;
      pageCnt = 0;
      do {
        while (pageStart_ < chunkSize_ && ++pageCnt <= pageCntPerBatch) {
          if (pageCnt == 1) {
            auto guessLeftPageCnt = std::min<uint64_t>(
                pageCntPerBatch, (chunkSize_ - pageStart_) / avgPageLen + 1);
            preloadedRepDefs_.emplace_back(raw_vector<char>(&pool_));
            preloadedRepDefs_.back().reserve(avgPageLen * guessLeftPageCnt);
          }
          preloadPageRepDefs(true);
        }
        if (pageCnt > pageCntPerBatch) {
          pageCnt = 0;
        }
      } while (pageStart_ < chunkSize_);
    }
  } else {
    while (pageStart_ < chunkSize_) {
      preloadPageRepDefs(false);
    }
  }

  // Reset the input to start of column chunk.
  std::vector<uint64_t> rewind = {0};
  pageStart_ = 0;
  dwio::common::PositionProvider position(rewind);
  inputStream_->seekToPosition(position);
  bufferStart_ = bufferEnd_ = nullptr;
  rowOfPage_ = 0;
  numRowsInPage_ = 0;
  pageData_ = nullptr;
  totalRefDefBytes_ = 0;
  pageOrdinal_ = 0;
  cryptoCtx_.startDecryptWithDictionaryPage = startWithDictinoaryPage;
}

void PageReader::decodeRepDefs(int32_t numTopLevelRows) {
  if (definitionLevels_.empty() && maxDefine_ > 0) {
    preloadRepDefs();
  }
  repDefBegin_ = repDefEnd_;
  int32_t numLevels = definitionLevels_.size();
  int32_t topFound = 0;
  int32_t i = repDefBegin_;

  auto foundTopLevel = [&]() {
    for (; i < numLevels; ++i) {
      if (repetitionLevels_[i] == 0) {
        ++topFound;
        if (topFound == numTopLevelRows + 1) {
          break;
        }
      }
    }
    repDefEnd_ = i;
  };

  if (maxRepeat_ > 0) {
    foundTopLevel();
  } else {
    repDefEnd_ = i + numTopLevelRows;
  }
  if (maxRepeat_ > 0 && maxDefine_ > 0) {
    // definitionLevels_ has been consumed, decode more if any
    while (repDefEnd_ == numLevels && topFound < numTopLevelRows + 1 &&
           !preloadedRepDefs_.empty()) {
      loadMoreRepDefs();
      numLevels = definitionLevels_.size();
      i = repDefEnd_;
      foundTopLevel();
      BOLT_CHECK(topFound == numTopLevelRows + 1 || repDefEnd_ == numLevels);
    }
    // after topFound done, left decoded rep/def is less than 1 page, decode
    // more
    if (!numLeavesInPage_.empty() &&
        numLevels - repDefEnd_ < numLeavesInPage_.back() &&
        topFound == numTopLevelRows + 1 && !preloadedRepDefs_.empty()) {
      loadMoreRepDefs();
    }
  }
}

void PageReader::loadMoreRepDefs() {
  BOLT_CHECK(maxDefine_ > 0);
  auto numLevels = definitionLevels_.size();
  auto topFoundCount = repDefEnd_ - repDefBegin_;
  auto copyElementCnt = numLevels - repDefBegin_;
  auto copySize = copyElementCnt * sizeof(int16_t);

  // [repDefBegin_, repDefEnd_) is valid in definitionLevels_
  // so we need to move it to [0, copyElementCnt)
  // repDefBegin_ == 0 means all in definitionLevels_ is valid
  // no need to do copy
  if (repDefBegin_ != 0) {
    // copyElementCnt == 0 means nothing to copy, skip
    if (copyElementCnt != 0) {
      if (numLevels - repDefBegin_ < repDefBegin_) {
        simd::memcpy(
            definitionLevels_.data(),
            definitionLevels_.data() + repDefBegin_,
            copySize);
        simd::memcpy(
            repetitionLevels_.data(),
            repetitionLevels_.data() + repDefBegin_,
            copySize);
      } else {
        raw_vector<int16_t> tmp(copyElementCnt, &pool_);
        // copy left definitionLevels_
        simd::memcpy(
            tmp.data(), definitionLevels_.data() + repDefBegin_, copySize);
        simd::memcpy(definitionLevels_.data(), tmp.data(), copySize);

        // copy left repetitionLevels_
        simd::memcpy(
            tmp.data(), repetitionLevels_.data() + repDefBegin_, copySize);
        simd::memcpy(repetitionLevels_.data(), tmp.data(), copySize);
      }
    }
    definitionLevels_.resize(copyElementCnt);
    repetitionLevels_.resize(copyElementCnt);
    repDefBegin_ = 0;
    repDefEnd_ = topFoundCount;
  }
  // build definitionLevels_/repetitionLevels_ from preloadedRepDefs_
  decodeRepDefsFromBuffer();
}

void PageReader::decodeRepDefsFromBuffer() {
  const auto& repDefData = preloadedRepDefs_.front();
  const auto* rawData = repDefData.data();
  constexpr int32_t WordBits = 64;
  size_t erasedBits = erasedLeafNullWords_ * WordBits;
  BOLT_CHECK_LE(numLeafNullsConsumed_, leafNullsSize_ + erasedBits);
  // clear consumed nulls
  if (numLeafNullsConsumed_ - erasedBits > WordBits) {
    auto consumedWords = bits::nwords(numLeafNullsConsumed_ - erasedBits) - 1;
    auto totalNullWords = bits::nwords(leafNullsSize_);
    auto unConsumedNullWords = totalNullWords - consumedWords;
    if (unConsumedNullWords > 0) {
      uint64_t* rawData = leafNulls_.data();
      size_t copyBytes = unConsumedNullWords * sizeof(uint64_t);

      /*
       * Intel FSRM Bug Detection Macro
       *
       * Background:
       * Intel CPUs with FSRM (Fast Short REP MOVSB) feature have a hardware bug
       * that causes memory corruption when source and destination pointers in
       * memcpy/memmove have specific offset ranges: (0,63] or (4GB, 4GB+63].
       *
       * Affected: Ice Lake 8336C, Sapphire Rapids (SPR), and all FSRM-enabled
       * CPUs Fixed in: glibc 2.33+
       * see
       * https://lore.kernel.org/all/20211101044955.2295495-1-goldstein.w.n@gmail.com/
       */
#if (defined(__x86_64__) || defined(__i386__)) && defined(__GLIBC__) && \
    (__GLIBC__ * 100 + __GLIBC_MINOR__ < 233)

      // Suppress false positive -Wrestrict warnings from GCC 12+
      // The memcpy calls below are intentionally designed to avoid overlap
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wrestrict"
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunknown-warning-option"
#pragma clang diagnostic ignored "-Wrestrict"
#endif
      if (unConsumedNullWords < consumedWords) {
        memcpy(rawData, rawData + consumedWords, copyBytes);
      } else {
        raw_vector<uint64_t> tmpBuffer;
        tmpBuffer.resize(unConsumedNullWords);
        uint64_t* tmpDest = tmpBuffer.data();
        memcpy(tmpDest, rawData + consumedWords, copyBytes);
        memcpy(rawData, tmpDest, copyBytes);
      }
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#elif defined(__clang__)
#pragma clang diagnostic pop
#endif
#else
      memmove(rawData, rawData + consumedWords, copyBytes);
#endif
      leafNulls_.resize(unConsumedNullWords);
      leafNullsSize_ -= consumedWords * WordBits;
      erasedLeafNullWords_ += consumedWords;
      BOLT_CHECK_EQ(leafNulls_.size(), bits::nwords(leafNullsSize_));
    }
  }

  while (rawData < repDefData.data() + repDefData.size()) {
    auto begin = definitionLevels_.size();
    int32_t numRepDefsInPage = readField<int32_t>(rawData);
    auto levels = definitionLevels_.size() + numRepDefsInPage;
    // decode repeat levels
    repetitionLevels_.resize(levels);
    uint32_t repeatLength = readField<int32_t>(rawData);
    auto repeatDecoder = ::arrow::util::RleDecoder(
        reinterpret_cast<const uint8_t*>(rawData),
        repeatLength,
        ::arrow::bit_util::NumRequiredBits(maxRepeat_));
    repeatDecoder.GetBatch(repetitionLevels_.data() + begin, numRepDefsInPage);
    rawData += repeatLength;

    definitionLevels_.resize(levels);
    auto defineLength = readField<uint32_t>(rawData);
    auto defineDecoder = ::arrow::util::RleDecoder(
        reinterpret_cast<const uint8_t*>(rawData),
        defineLength,
        ::arrow::bit_util::NumRequiredBits(maxDefine_));
    defineDecoder.GetBatch(definitionLevels_.data() + begin, numRepDefsInPage);
    rawData += defineLength;

    leafNulls_.resize(bits::nwords(leafNullsSize_ + numRepDefsInPage));
    auto numLeaves = getLengthsAndNulls(
        LevelMode::kNulls,
        leafInfo_,
        begin,
        begin + numRepDefsInPage,
        numRepDefsInPage,
        nullptr,
        leafNulls_.data(),
        leafNullsSize_);
    leafNullsSize_ += numLeaves;
    numLeavesInPage_.push_back(numLeaves);
  }
  preloadedRepDefs_.pop_front();
}

int32_t PageReader::getLengthsAndNulls(
    LevelMode mode,
    const arrow::LevelInfo& info,
    int32_t begin,
    int32_t end,
    int32_t maxItems,
    int32_t* lengths,
    uint64_t* nulls,
    int32_t nullsStartIndex) const {
  arrow::ValidityBitmapInputOutput bits;
  bits.values_read_upper_bound = maxItems;
  bits.values_read = 0;
  bits.null_count = 0;
  bits.valid_bits = reinterpret_cast<uint8_t*>(nulls);
  bits.valid_bits_offset = nullsStartIndex;

  switch (mode) {
    case LevelMode::kNulls:
      DefLevelsToBitmap(
          definitionLevels_.data() + begin, end - begin, info, &bits);
      break;
    case LevelMode::kList: {
      arrow::DefRepLevelsToList(
          definitionLevels_.data() + begin,
          repetitionLevels_.data() + begin,
          end - begin,
          info,
          &bits,
          lengths);
      // Convert offsets to lengths.
      for (auto i = 0; i < bits.values_read; ++i) {
        lengths[i] = lengths[i + 1] - lengths[i];
      }
      break;
    }
    case LevelMode::kStructOverLists: {
      DefRepLevelsToBitmap(
          definitionLevels_.data() + begin,
          repetitionLevels_.data() + begin,
          end - begin,
          info,
          &bits);
      break;
    }
  }
  return bits.values_read;
}

void PageReader::makeDecoder() {
  auto parquetType = type_->parquetType_.value();
  switch (encoding_) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY:
      dictionaryIdDecoder_ = std::make_unique<RleBpDataDecoder>(
          pageData_ + 1, pageData_ + encodedDataSize_, pageData_[0]);
      decoderSet_ = true;
      break;
    case Encoding::PLAIN:
      switch (parquetType) {
        case thrift::Type::BOOLEAN:
          booleanDecoder_ = std::make_unique<BooleanDecoder>(
              pageData_, pageData_ + encodedDataSize_);
          break;
        case thrift::Type::BYTE_ARRAY:
          stringDecoder_ = std::make_unique<StringDecoder>(
              pageData_, pageData_ + encodedDataSize_);
          break;
        case thrift::Type::FIXED_LEN_BYTE_ARRAY:
          if (type_->type()->isVarbinary() || type_->type()->isVarchar()) {
            stringDecoder_ = std::make_unique<StringDecoder>(
                pageData_, pageData_ + encodedDataSize_, type_->typeLength_);
          } else {
            directDecoder_ =
                std::make_unique<dwio::common::DirectDecoder<true>>(
                    std::make_unique<dwio::common::SeekableArrayInputStream>(
                        pageData_, encodedDataSize_),
                    false,
                    type_->typeLength_,
                    true);
          }
          break;
        default: {
          directDecoder_ = std::make_unique<dwio::common::DirectDecoder<true>>(
              std::make_unique<dwio::common::SeekableArrayInputStream>(
                  pageData_, encodedDataSize_),
              false,
              parquetTypeBytes(type_->parquetType_.value()));
        }
      }
      decoderSet_ = true;
      break;
    case Encoding::DELTA_BINARY_PACKED:
      switch (parquetType) {
        case thrift::Type::INT32:
        case thrift::Type::INT64:
          deltaBpDecoder_ = std::make_unique<DeltaBpDecoder>(pageData_);
          break;
        default:
          BOLT_UNSUPPORTED(
              "DELTA_BINARY_PACKED decoder only supports INT32 and INT64");
      }
      break;
    default:
      BOLT_UNSUPPORTED("Encoding not supported yet: {}", encoding_);
      break;
  }
}

void PageReader::skip(int64_t numRows) {
  if (!numRows && firstUnvisited_ != rowOfPage_ + numRowsInPage_) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (firstUnvisited_ + numRows >= rowOfPage_ + numRowsInPage_) {
    seekToPage(firstUnvisited_ + numRows);
    if (hasChunkRepDefs_) {
      numLeafNullsConsumed_ = rowOfPage_;
    }
    toSkip -= rowOfPage_ - firstUnvisited_;
  }
  firstUnvisited_ += numRows;

  // Skip nulls
  toSkip = skipNulls(toSkip);

  if (toSkip == 0) {
    //    When the skip() hits the end of the page, we can return directly
    //    without going into the decoders' skip.
    return;
  }

  // Skip the decoder
  if (isDictionary()) {
    dictionaryIdDecoder_->skip(toSkip);
  } else if (directDecoder_) {
    directDecoder_->skip(toSkip);
  } else if (stringDecoder_) {
    stringDecoder_->skip(toSkip);
  } else if (booleanDecoder_) {
    booleanDecoder_->skip(toSkip);
  } else if (deltaBpDecoder_) {
    deltaBpDecoder_->skip(toSkip);
  } else if (!decoderSet_ && chunkSize_ <= pageStart_) {
    // There is a chance that we seek to the end of column chunk
    // without read any PageData , no skip for decoder
  } else {
    BOLT_FAIL("No decoder to skip");
  }
}

int32_t PageReader::skipNulls(int32_t numValues) {
  if (!defineDecoder_ && isTopLevel_) {
    return numValues;
  }
  BOLT_CHECK(1 == maxDefine_ || !leafNulls_.empty());
  dwio::common::ensureCapacity<bool>(tempNulls_, numValues, &pool_);
  tempNulls_->setSize(0);
  if (isTopLevel_) {
    bool allOnes;
    defineDecoder_->readBits(
        numValues, tempNulls_->asMutable<uint64_t>(), &allOnes);
    if (allOnes) {
      return numValues;
    }
  } else {
    readNulls(numValues, tempNulls_);
  }
  auto words = tempNulls_->as<uint64_t>();
  return bits::countBits(words, 0, numValues);
}

void PageReader::skipNullsOnly(int64_t numRows) {
  if (!numRows && firstUnvisited_ != rowOfPage_ + numRowsInPage_) {
    // Return if no skip and position not at end of page or before first page.
    return;
  }
  auto toSkip = numRows;
  if (firstUnvisited_ + numRows >= rowOfPage_ + numRowsInPage_) {
    seekToPage(firstUnvisited_ + numRows);
    firstUnvisited_ += numRows;
    toSkip = firstUnvisited_ - rowOfPage_;
  } else {
    firstUnvisited_ += numRows;
  }

  // Skip nulls
  skipNulls(toSkip);
}

void PageReader::readNullsOnly(int64_t numValues, BufferPtr& buffer) {
  BOLT_CHECK(!maxRepeat_);
  auto toRead = numValues;
  if (buffer) {
    dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  }
  nullConcatenation_.reset(buffer);
  while (toRead) {
    auto availableOnPage = rowOfPage_ + numRowsInPage_ - firstUnvisited_;
    if (!availableOnPage) {
      seekToPage(firstUnvisited_);
      availableOnPage = numRowsInPage_;
    }
    auto numRead = std::min(availableOnPage, toRead);
    auto nulls = readNulls(numRead, nullsInReadRange_);
    toRead -= numRead;
    nullConcatenation_.append(nulls, 0, numRead);
    firstUnvisited_ += numRead;
  }
  buffer = nullConcatenation_.buffer();
}

const uint64_t* FOLLY_NULLABLE
PageReader::readNulls(int32_t numValues, BufferPtr& buffer) {
  if (maxDefine_ == 0) {
    buffer = nullptr;
    return nullptr;
  }
  dwio::common::ensureCapacity<bool>(buffer, numValues, &pool_);
  if (isTopLevel_) {
    BOLT_CHECK_EQ(1, maxDefine_);
    bool allOnes;
    defineDecoder_->readBits(
        numValues, buffer->asMutable<uint64_t>(), &allOnes);
    return allOnes ? nullptr : buffer->as<uint64_t>();
  }
  bits::copyBits(
      leafNulls_.data(),
      numLeafNullsConsumed_ - erasedLeafNullWords_ * 64,
      buffer->asMutable<uint64_t>(),
      0,
      numValues);
  numLeafNullsConsumed_ += numValues;
  return buffer->as<uint64_t>();
}

void PageReader::startVisit(folly::Range<const vector_size_t*> rows) {
  visitorRows_ = rows.data();
  numVisitorRows_ = rows.size();
  currentVisitorRow_ = 0;
  initialRowOfPage_ = rowOfPage_;
  visitBase_ = firstUnvisited_;
}

bool PageReader::rowsForPage(
    dwio::common::SelectiveColumnReader& reader,
    bool hasFilter,
    bool mayProduceNulls,
    folly::Range<const vector_size_t*>& rows,
    const uint64_t* FOLLY_NULLABLE& nulls) {
  if (currentVisitorRow_ == numVisitorRows_) {
    return false;
  }
  int32_t numToVisit;
  // Check if the first row to go to is in the current page. If not, seek to
  // the page that contains the row.
  auto rowZero = visitBase_ + visitorRows_[currentVisitorRow_];
  if (rowZero >= rowOfPage_ + numRowsInPage_) {
    seekToPage(rowZero);
    if (hasChunkRepDefs_) {
      numLeafNullsConsumed_ = rowOfPage_;
    }
  }
  auto& scanState = reader.scanState();
  if (isDictionary()) {
    if (scanState.dictionary.values != dictionary_.values) {
      scanState.dictionary = dictionary_;
      if (hasFilter) {
        makeFilterCache(scanState);
      }
      scanState.updateRawState();
    }
  } else {
    if (scanState.dictionary.values) {
      // If there are previous pages in the current read, nulls read
      // from them are in 'nullConcatenation_' Put this into the
      // reader for the time of dedictionarizing so we don't read
      // undefined dictionary indices.
      if (mayProduceNulls && reader.numValues()) {
        reader.setNulls(nullConcatenation_.buffer());
      }
      reader.dedictionarize();
      // The nulls across all pages are in nullConcatenation_. Clear
      // the nulls and let the prepareNulls below reserve nulls for
      // the new page.
      reader.setNulls(nullptr);
      scanState.dictionary.clear();
    }
  }

  // Then check how many of the rows to visit are on the same page as the
  // current one.
  int32_t firstOnNextPage = rowOfPage_ + numRowsInPage_ - visitBase_;
  if (firstOnNextPage > visitorRows_[numVisitorRows_ - 1]) {
    // All the remaining rows are on this page.
    numToVisit = numVisitorRows_ - currentVisitorRow_;
  } else {
    // Find the last row in the rows to visit that is on this page.
    auto rangeLeft = folly::Range<const int32_t*>(
        visitorRows_ + currentVisitorRow_,
        numVisitorRows_ - currentVisitorRow_);
    auto it =
        std::lower_bound(rangeLeft.begin(), rangeLeft.end(), firstOnNextPage);
    BOLT_CHECK(it != rangeLeft.end());
    BOLT_CHECK(it != rangeLeft.begin());
    numToVisit = it - (visitorRows_ + currentVisitorRow_);
  }
  // If the page did not change and this is the first call, we can return a
  // view on the original visitor rows.
  if (rowOfPage_ == initialRowOfPage_ && currentVisitorRow_ == 0) {
    nulls =
        readNulls(visitorRows_[numToVisit - 1] + 1, reader.nullsInReadRange());
    rowNumberBias_ = 0;
    rows = folly::Range<const vector_size_t*>(visitorRows_, numToVisit);
  } else {
    // We scale row numbers to be relative to first on this page.
    auto pageOffset = rowOfPage_ - visitBase_;
    rowNumberBias_ = visitorRows_[currentVisitorRow_];
    skip(rowNumberBias_ - pageOffset);
    // The decoder is positioned at 'visitorRows_[currentVisitorRow_']'
    // We copy the rows to visit with a bias, so that the first to visit has
    // offset 0.
    rowsCopy_->resize(numToVisit);
    auto copy = rowsCopy_->data();
    // Subtract 'rowNumberBias_' from the rows to visit on this page.
    // 'copy' has a writable tail of SIMD width, so no special case for end of
    // loop.
    for (auto i = 0; i < numToVisit; i += xsimd::batch<int32_t>::size) {
      auto numbers = xsimd::batch<int32_t>::load_unaligned(
                         &visitorRows_[i + currentVisitorRow_]) -
          rowNumberBias_;
      numbers.store_unaligned(copy);
      copy += xsimd::batch<int32_t>::size;
    }
    nulls = readNulls(rowsCopy_->back() + 1, reader.nullsInReadRange());
    rows = folly::Range<const vector_size_t*>(
        rowsCopy_->data(), rowsCopy_->size());
  }
  reader.prepareNulls(rows, nulls != nullptr, currentVisitorRow_);
  currentVisitorRow_ += numToVisit;
  firstUnvisited_ = visitBase_ + visitorRows_[currentVisitorRow_ - 1] + 1;
  return true;
}

const VectorPtr& PageReader::dictionaryValues(const TypePtr& type) {
  if (!dictionaryValues_) {
    switch (type->kind()) {
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.strings});
        break;
      case TypeKind::HUGEINT:
        dictionaryValues_ = std::make_shared<FlatVector<int128_t>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.values});
        break;
      case TypeKind::BIGINT:
        dictionaryValues_ = std::make_shared<FlatVector<int64_t>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.values});
        break;

      case TypeKind::INTEGER:
        dictionaryValues_ = std::make_shared<FlatVector<int32_t>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.values});
        break;

      case TypeKind::SMALLINT:
        dictionaryValues_ = std::make_shared<FlatVector<int16_t>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.values});
        break;

      case TypeKind::TINYINT:
        dictionaryValues_ = std::make_shared<FlatVector<int8_t>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.values});
        break;

      case TypeKind::BOOLEAN:
        dictionaryValues_ = std::make_shared<FlatVector<bool>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.values});
        break;

      case TypeKind::REAL:
        dictionaryValues_ = std::make_shared<FlatVector<float>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.values});
        break;

      case TypeKind::DOUBLE:
        dictionaryValues_ = std::make_shared<FlatVector<double>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.values});
        break;

      case TypeKind::TIMESTAMP:
        dictionaryValues_ = std::make_shared<FlatVector<Timestamp>>(
            &pool_,
            type,
            nullptr,
            dictionary_.numValues,
            dictionary_.values,
            std::vector<BufferPtr>{dictionary_.values});
        break;

      default:
        BOLT_FAIL(
            "Unsupported type for dictionary values: {}", type->toString());
    }
  }
  return dictionaryValues_;
}

void PageReader::decryptPageData(int32_t& compressedLen) {
  if (decryptionBuffer_) {
    AlignedBuffer::reallocate<uint8_t>(
        &decryptionBuffer_,
        compressedLen - cryptoCtx_.dataDecryptor->CiphertextSizeDelta());
  } else {
    decryptionBuffer_ = AlignedBuffer::allocate<uint8_t>(
        compressedLen - cryptoCtx_.dataDecryptor->CiphertextSizeDelta(),
        &pool_);
  }
  compressedLen = cryptoCtx_.dataDecryptor->Decrypt(
      reinterpret_cast<const uint8_t*>(pageData_),
      compressedLen,
      const_cast<uint8_t*>(decryptionBuffer_->as<uint8_t>()),
      decryptionBuffer_->size());
  pageData_ = decryptionBuffer_->as<char>();
}

} // namespace bytedance::bolt::parquet
