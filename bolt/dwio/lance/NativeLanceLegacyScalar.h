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

#include <functional>
#include <string_view>

#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/dwio/lance/proto/lance_encodings_v2_0.pb.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

using NativeLanceLegacyRead =
    std::function<BufferPtr(uint64_t offset, uint64_t length)>;
using NativeLanceLegacyCompressedRead = std::function<BufferPtr(
    std::string_view scheme,
    uint64_t compressedOffset,
    uint64_t compressedLength,
    uint64_t decodedOffset,
    uint64_t decodedLength)>;

VectorPtr decodeLegacyPrimitivePage(
    const TypePtr& type,
    std::string_view logicalType,
    const ::lance::encodings::ArrayEncoding& encoding,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed);

void decodeLegacyFixedWidthValues(
    const TypePtr& type,
    std::string_view logicalType,
    uint64_t bitsPerValue,
    const char* source,
    uint64_t count,
    uint64_t outputOffset,
    VectorPtr& result);

void decodeLegacyBitmapValues(
    const char* source,
    uint64_t sourceBitOffset,
    uint64_t count,
    uint64_t outputOffset,
    VectorPtr& result);

void applyLegacyValidityBitmap(
    const uint8_t* source,
    uint64_t sourceBitOffset,
    uint64_t count,
    uint64_t outputOffset,
    BaseVector& result);

void setLegacyAllNull(
    uint64_t outputOffset,
    uint64_t count,
    BaseVector& result);

void decodeLegacyBitpackedValues(
    const TypePtr& type,
    std::string_view logicalType,
    const ::lance::encodings::Bitpacked& bitpacked,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t outputOffset,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    VectorPtr& result);

void decodeLegacyBitpackedForNonNegValues(
    const TypePtr& type,
    std::string_view logicalType,
    const ::lance::encodings::BitpackedForNonNeg& bitpacked,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t outputOffset,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    VectorPtr& result);

VectorPtr tryWrapLegacyRawFlatValues(
    const TypePtr& type,
    std::string_view logicalType,
    uint64_t bitsPerValue,
    BufferPtr values,
    uint64_t count,
    memory::MemoryPool& pool);

} // namespace bytedance::bolt::lance::reader
