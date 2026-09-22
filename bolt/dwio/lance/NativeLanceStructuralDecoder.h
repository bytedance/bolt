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

#include "bolt/dwio/lance/NativeLanceBlobResolver.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

/// Decodes a complete v2.1-v2.3 structural page. Range slicing is performed
/// by the caller after the structural layers have been reconstructed.
VectorPtr decodeLanceStructuralPage(
    const TypePtr& type,
    std::string_view leafLogicalType,
    const std::vector<uint32_t>& fixedSizeDimensions,
    const std::vector<std::string>& packedChildLogicalTypes,
    const ::lance::file::v2::ColumnMetadata& column,
    const ::lance::file::v2::ColumnMetadata::Page& page,
    const ::lance::encodings21::PageLayout& layout,
    memory::MemoryPool& pool,
    const std::shared_ptr<const NativeLanceBlobResolver>& blobResolver,
    std::string_view sourceDataFile,
    const std::function<
        void(const std::vector<std::pair<uint64_t, uint64_t>>&)>& prefetch,
    const std::function<BufferPtr(uint64_t, uint64_t)>& read);

bool lanceStructuralLayoutHasCompression(
    const ::lance::encodings21::PageLayout& layout);

} // namespace bytedance::bolt::lance::reader
