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

#include <exception>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/ParallelFor.h"
#include "bolt/dwio/lance/NativeLanceColumnRequest.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::lance::reader {

class NativeLanceMetadata;
class NativeLancePageSource;

enum class NativeLanceReadStage { kRowAligned, kOffsetDependent };

enum class NativeLanceColumnKind : uint8_t {
  kConstant,
  kScalar,
  kBinary,
  kDictionary,
  kList,
  kMap,
  kStruct,
  kFixedSizeList,
  kPackedStruct,
  kBlob,
};

enum class NativeLanceColumnState : uint8_t {
  kIdle,
  kPlanningPages,
  kWaitingPages,
  kAssembling,
  kReady,
  kConsumed,
  kFailed,
  kCancelled,
};

class NativeLanceColumnReader {
 public:
  virtual ~NativeLanceColumnReader() = default;

  static std::unique_ptr<NativeLanceColumnReader> buildFileColumn(
      const NativeLanceMetadata& metadata,
      uint32_t fileColumnIndex);

  virtual bool readFromFile() const = 0;
  virtual NativeLanceColumnKind kind() const = 0;
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

  const RowTypePtr& outputType() const {
    return outputType_;
  }

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

  NativeLanceColumnKind childKind(column_index_t channel) const {
    return children_.at(channel)->kind();
  }

 private:
  NativeLanceRootColumnReader(
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

/// Batch-local state for executing an immutable column-reader tree.
class NativeLanceColumnReadTask {
 public:
  NativeLanceColumnReadTask(
      const NativeLanceRootColumnReader& reader,
      NativeLanceColumnRequest request,
      bool primaryRangesPlanned = false);

  void plan(NativeLancePageSource& decoder);
  void decode(NativeLancePageSource& decoder, memory::MemoryPool& pool);
  VectorPtr consume();
  void cancel(NativeLancePageSource& decoder);

  NativeLanceColumnState state() const {
    return state_;
  }

 private:
  [[noreturn]] void rethrowFailure() const;

  const NativeLanceRootColumnReader& reader_;
  const NativeLanceColumnRequest request_;
  VectorPtr result_;
  std::exception_ptr failure_;
  NativeLanceColumnState state_{NativeLanceColumnState::kIdle};
};

} // namespace bytedance::bolt::lance::reader
