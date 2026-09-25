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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/ParallelFor.h"
#include "bolt/dwio/lance/NativeLanceColumnRequest.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::lance::reader {

class NativeLancePageSource;

VectorPtr decodeNativeLanceStructuralColumn(
    const NativeLancePageSource& source,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    const NativeLanceMetadata::StructuralField& field,
    uint64_t rowStart,
    uint64_t rowCount);

enum class NativeLanceReadStage { kRowAligned, kOffsetDependent };

bool nativeLanceTypeRequiresDeferredRead(const TypePtr& type);

class NativeLanceColumnReader {
 public:
  virtual ~NativeLanceColumnReader() = default;

  static std::unique_ptr<NativeLanceColumnReader> buildFileColumn(
      const NativeLanceMetadata& metadata,
      uint32_t fileColumnIndex);

  virtual bool readFromFile() const = 0;
  virtual NativeLanceReadStage readStage() const = 0;
  virtual uint32_t fileColumnIndex() const = 0;
  virtual const TypePtr& type() const = 0;

  virtual void plan(
      NativeLancePageSource& source,
      const NativeLanceColumnRequest& request) const = 0;

  virtual VectorPtr read(
      NativeLancePageSource& source,
      const NativeLanceColumnRequest& request,
      memory::MemoryPool& pool,
      bool rangesPlanned) const = 0;
};

class NativeLanceRootColumnReader {
 public:
  static std::unique_ptr<NativeLanceRootColumnReader> buildRoot(
      const NativeLanceMetadata& metadata,
      const dwio::common::RowReaderOptions& options);

  VectorPtr read(
      NativeLancePageSource& source,
      const NativeLanceColumnRequest& request,
      memory::MemoryPool& pool,
      bool primaryRangesPlanned = false,
      const std::unordered_map<uint32_t, VectorPtr>* predecodedColumns =
          nullptr) const;

  void planRead(
      NativeLancePageSource& source,
      const NativeLanceColumnRequest& request) const;

  std::vector<uint32_t> fileColumnIndices() const;

 private:
  struct ReadColumn {
    uint32_t fileColumnIndex;
    const NativeLanceColumnReader* reader;
  };

  NativeLanceRootColumnReader(
      RowTypePtr outputType,
      std::vector<std::unique_ptr<NativeLanceColumnReader>> children,
      std::shared_ptr<folly::Executor> decodingExecutor,
      size_t decodingParallelismFactor);

  void initializeReadPlan();

  RowTypePtr outputType_;
  std::vector<std::unique_ptr<NativeLanceColumnReader>> children_;
  std::vector<ReadColumn> rowAlignedReadPlan_;
  std::vector<ReadColumn> offsetDependentReadPlan_;
  std::vector<uint32_t> fileColumnIndices_;
  std::shared_ptr<folly::Executor> decodingExecutor_;
  size_t decodingParallelismFactor_;
};

} // namespace bytedance::bolt::lance::reader
