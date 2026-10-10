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

#include <cstdint>
#include <functional>
#include <string_view>
#include <vector>

#include "bolt/dwio/lance/NativeLanceBlobResolver.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

class NativeLanceStructuralPagePlan;

struct NativeLancePageKey {
  uint32_t physicalColumn;
  int32_t pageIndex;

  bool operator==(const NativeLancePageKey& other) const {
    return physicalColumn == other.physicalColumn &&
        pageIndex == other.pageIndex;
  }
};

struct NativeLanceStructuralPageRequest {
  NativeLancePageKey key;
  TypePtr type;
  std::string_view logicalType;
  std::vector<uint32_t> fixedSizeDimensions;
  std::vector<std::string> packedChildLogicalTypes;
  uint64_t localRowStart;
  uint64_t rowCount;
};

/// Owns the state and page-local decode of one v2.1+ structural page.
class NativeLanceStructuralPageReader {
 public:
  using Prefetch =
      std::function<void(const std::vector<std::pair<uint64_t, uint64_t>>&)>;
  using Read = std::function<BufferPtr(uint64_t, uint64_t)>;

  NativeLanceStructuralPageReader(
      NativeLanceStructuralPageRequest request,
      const NativeLanceMetadata& metadata,
      memory::MemoryPool& pool,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
      std::string_view sourceDataFile,
      Prefetch prefetch,
      Read read,
      std::shared_ptr<const NativeLanceStructuralPagePlan> plan = nullptr);

  VectorPtr read();

 private:
  NativeLanceStructuralPageRequest request_;
  const NativeLanceMetadata& metadata_;
  memory::MemoryPool& pool_;
  std::shared_ptr<const NativeLanceBlobResolver> blobResolver_;
  std::string sourceDataFile_;
  Prefetch prefetch_;
  Read read_;
  std::shared_ptr<const NativeLanceStructuralPagePlan> plan_;
};

} // namespace bytedance::bolt::lance::reader
