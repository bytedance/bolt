/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/Reader.h"
#include "bolt/dwio/common/ReaderFactory.h"

namespace bytedance::bolt::txt::reader {
class ReaderBase {
 public:
  ReaderBase(
      std::unique_ptr<dwio::common::BufferedInput>,
      const dwio::common::ReaderOptions& options);

  virtual ~ReaderBase() = default;

  memory::MemoryPool& getMemoryPool() const {
    return pool_;
  }

  dwio::common::BufferedInput& bufferedInput() const {
    return *input_;
  }

  uint64_t fileLength() const {
    return fileLength_;
  }

  const std::shared_ptr<const RowType>& schema() const {
    return schema_;
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& schemaWithId() {
    return schemaWithId_;
  }

  bool isFileColumnNamesReadAsLowerCase() const {
    return options_.isFileColumnNamesReadAsLowerCase();
  }

  const dwio::common::ReaderOptions& getReaderOptions() const {
    return options_;
  }

 private:
  memory::MemoryPool& pool_;
  // Copy of options. Must be owned by 'this'.
  const dwio::common::ReaderOptions options_;
  std::shared_ptr<bolt::dwio::common::BufferedInput> input_;
  uint64_t fileLength_;
  RowTypePtr schema_;
  std::shared_ptr<const dwio::common::TypeWithId> schemaWithId_;

  // Map from row group index to pre-created loading BufferedInput.
  std::unordered_map<uint32_t, std::shared_ptr<dwio::common::BufferedInput>>
      inputs_;
};

class TxtRowReader : public dwio::common::RowReader {
 public:
  TxtRowReader(
      const std::shared_ptr<ReaderBase>& readerBase,
      const dwio::common::RowReaderOptions& options);
  ~TxtRowReader() override;

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  uint64_t next(
      uint64_t size,
      bolt::VectorPtr& result,
      const dwio::common::Mutation* = nullptr) override;

  uint64_t skip(uint64_t skipSize) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

  bool allPrefetchIssued() const override {
    //  Allow opening the next split while this is reading.
    return true;
  }

  // Checks if the specific row group is buffered.
  // Returns false if the row group is not loaded into buffer
  // or the buffered data has been evicted.

 protected:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class TxtReader : public dwio::common::Reader {
 public:
  std::optional<uint64_t> numberOfRows() const override;

  const bolt::RowTypePtr& rowType() const override;

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override {
    return nullptr;
  }

  TxtReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options);

  ~TxtReader() override = default;

 private:
  std::shared_ptr<ReaderBase> readerBase_;
};

class TxtReaderFactory : public dwio::common::ReaderFactory {
 public:
  TxtReaderFactory() : ReaderFactory(dwio::common::FileFormat::TEXT) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options) override;
};

} // namespace bytedance::bolt::txt::reader
