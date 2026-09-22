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
#include "bolt/dwio/lance/NativeLanceDecoder.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::lance::reader {

enum class NativeLanceReadStage { kRowAligned, kOffsetDependent };

class NativeLanceColumnReader {
 public:
  virtual ~NativeLanceColumnReader() = default;

  virtual bool readFromFile() const = 0;
  virtual NativeLanceReadStage readStage() const = 0;
  virtual uint32_t fileColumnIndex() const = 0;
  virtual const TypePtr& type() const = 0;
  virtual VectorPtr read(
      NativeLanceDecoder& decoder,
      uint64_t rowStart,
      uint64_t rowCount,
      memory::MemoryPool& pool) const = 0;
};

class NativeLanceStructColumnReader {
 public:
  static std::unique_ptr<NativeLanceStructColumnReader> buildRoot(
      const RowTypePtr& fileType,
      const dwio::common::RowReaderOptions& options);

  const RowTypePtr& outputType() const {
    return outputType_;
  }

  VectorPtr read(
      NativeLanceDecoder& decoder,
      uint64_t rowStart,
      uint64_t rowCount,
      memory::MemoryPool& pool) const;

  void planRead(
      NativeLanceDecoder& decoder,
      uint64_t rowStart,
      uint64_t rowCount) const;

 private:
  NativeLanceStructColumnReader(
      RowTypePtr outputType,
      std::vector<std::unique_ptr<NativeLanceColumnReader>> children,
      std::shared_ptr<folly::Executor> decodingExecutor,
      size_t decodingParallelismFactor);

  std::vector<uint32_t> readColumns(NativeLanceReadStage stage) const;

  RowTypePtr outputType_;
  std::vector<std::unique_ptr<NativeLanceColumnReader>> children_;
  std::shared_ptr<folly::Executor> decodingExecutor_;
  size_t decodingParallelismFactor_;
};

} // namespace bytedance::bolt::lance::reader
