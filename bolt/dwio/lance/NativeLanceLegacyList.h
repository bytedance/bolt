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

#include "bolt/dwio/lance/NativeLanceLegacyScalar.h"

namespace bytedance::bolt::lance::reader {

using NativeLanceDecodePhysicalColumn = std::function<VectorPtr(
    const TypePtr& type,
    std::string_view logicalType,
    uint32_t physicalColumn,
    uint64_t rowStart,
    uint64_t rowCount)>;

using NativeLancePrefetchPhysicalColumn = std::function<void(
    const TypePtr& type,
    uint32_t physicalColumn,
    uint64_t rowStart,
    uint64_t rowCount)>;

VectorPtr decodeLegacyListPage(
    const TypePtr& type,
    const ::lance::encodings::ArrayEncoding& encoding,
    uint32_t physicalColumn,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const NativeLanceMetadata& metadata,
    uint64_t localStart,
    uint64_t localCount,
    uint64_t itemsOffset,
    memory::MemoryPool& pool,
    const NativeLanceLegacyRead& read,
    const NativeLanceLegacyCompressedRead& readCompressed,
    const NativeLancePrefetchPhysicalColumn& prefetchChild,
    const NativeLanceDecodePhysicalColumn& decodeChild);

} // namespace bytedance::bolt::lance::reader
