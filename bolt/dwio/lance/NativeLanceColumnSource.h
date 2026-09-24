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

#include "bolt/dwio/lance/NativeLanceColumnRequest.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

/// Narrow migration boundary between logical ColumnReaders and physical page
/// decoding. NativeLanceDecoder implements this adapter while encoding
/// families move into PageReader implementations.
class NativeLanceColumnSource {
 public:
  virtual ~NativeLanceColumnSource() = default;

  virtual void cancel() = 0;

  virtual void planColumns(
      const std::vector<uint32_t>& columnIndices,
      const NativeLanceColumnRequest& request) const = 0;

  virtual bool supportsConcurrentDecoding() const = 0;

  virtual bool hasCompressedColumn(
      uint32_t columnIndex,
      uint64_t rowStart,
      uint64_t rowCount) const = 0;

  virtual VectorPtr decodeColumn(
      uint32_t columnIndex,
      const NativeLanceColumnRequest& request,
      bool rangesPlanned) const = 0;
};

} // namespace bytedance::bolt::lance::reader
