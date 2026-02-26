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

#pragma once

#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/SelectiveColumnReaderInternal.h"
namespace bytedance::bolt::dwio::common {

class SelectiveStructColumnReaderBase : public SelectiveColumnReader {
 public:
  void resetFilterCaches() override {
    for (auto& child : children_) {
      child->resetFilterCaches();
    }
  }

  void resetWhenFilterAll() override {
    SelectiveColumnReader::resetWhenFilterAll();
    for (auto child : children_) {
      if (child) {
        child->resetWhenFilterAll();
      }
    }
  }

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const Mutation*) override;

  void filterRowGroups(
      uint64_t rowGroupSize,
      const dwio::common::StatsContext& context,
      FormatData::FilterRowGroupsResult&,
      dwio::common::BufferedInput& input) const override;

  void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls)
      override;

  void getValues(const RowSet& rows, VectorPtr* result) override;

  uint64_t numReads() const {
    return numReads_;
  }

  vector_size_t lazyVectorReadOffset() const {
    return lazyVectorReadOffset_;
  }

  /// Advance field reader to the row group closest to specified offset by
  /// calling seekToRowGroup.
  virtual void advanceFieldReader(
      SelectiveColumnReader* reader,
      int64_t offset) = 0;

  // Returns the nulls bitmap from reading this. Used in LazyVector loaders.
  const uint64_t* nulls() const {
    return nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  }

  void setReadOffsetRecursive(int64_t readOffset) override {
    readOffset_ = readOffset;
    for (auto& child : children_) {
      child->setReadOffsetRecursive(readOffset);
    }
  }

  void setIsTopLevel() override {
    isTopLevel_ = true;
    if (!formatData_->hasNulls()) {
      for (auto& child : children_) {
        child->setIsTopLevel();
      }
    }
  }

  const std::vector<SelectiveColumnReader*>& children() const override {
    return children_;
  }

  // Sets 'rows' as the set of rows for which 'this' or its children
  // may be loaded as LazyVectors. When a struct is loaded as lazy,
  // its children will be lazy if the struct does not add nulls. The
  // children will reference the struct reader, which must have a live
  // and up-to-date set of rows for which children can be loaded.
  void setLoadableRows(const RowSet& rows) {
    setOutputRows(rows);
    inputRows_ = outputRows_;
  }

  const std::string& debugString() const {
    return debugString_;
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& requestedTypeWithId()
      const {
    return requestedType_;
  }

 protected:
  // The subscript of childSpecs will be set to this value if the column is
  // constant (either explicitly or because it's missing).
  static constexpr int32_t kConstantChildSpecSubscript = -1;

  SelectiveStructColumnReaderBase(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      FormatParams& params,
      bolt::common::ScanSpec& scanSpec,
      bool isRoot = false)
      : SelectiveColumnReader(fileType->type(), fileType, params, scanSpec),
        columnReaderOptions_(columnReaderOptions),
        requestedType_(requestedType),
        debugString_(
            getExceptionContext().message(BoltException::Type::kSystem)),
        isRoot_(isRoot) {}

  // Records the number of nulls added by 'this' between the end
  // position of each child reader and the end of the range of
  // 'read(). This must be done also if a child is not read so that we
  // know how much to skip when seeking forward within the row group.
  void recordParentNullsInChildren(int64_t offset, const RowSet& rows);

  bool hasMutation() const override {
    return hasMutation_;
  }

  bool isChildConstant(const bolt::common::ScanSpec& childSpec) const;

  bool isChildMissing(const bolt::common::ScanSpec& childSpec) const;

  const dwio::common::ColumnReaderOptions columnReaderOptions_;
  const std::shared_ptr<const dwio::common::TypeWithId> requestedType_;

  std::vector<SelectiveColumnReader*> children_;

  // Sequence number of output batch. Checked against ColumnLoaders
  // created by 'this' to verify they are still valid at load.
  uint64_t numReads_ = 0;

  vector_size_t lazyVectorReadOffset_;
  // Represents the index of the first row of the current row group, relative to
  // the start of the file. This is only used in the top-level struct reader.
  int64_t rowGroupOffset_ = 0;

  // Dense set of rows to read in next().
  raw_vector<vector_size_t> rows_;

  const Mutation* mutation_ = nullptr;

  // After read() call mutation_ could go out of scope.  Need to keep this
  // around for lazy columns.
  bool hasMutation_ = false;

  // Context information obtained from ExceptionContext. Stored here
  // so that LazyVector readers under this can add this to their
  // ExceptionContext. Allows contextualizing reader errors to split
  // and query. Set at construction, which takes place on first
  // use. If no ExceptionContext is in effect, this is "".
  const std::string debugString_;

  // Whether or not this is the root Struct that represents entire rows of the
  // table.
  const bool isRoot_;
};

struct SelectiveStructColumnReader : SelectiveStructColumnReaderBase {
  using SelectiveStructColumnReaderBase::SelectiveStructColumnReaderBase;

  void addChild(std::unique_ptr<SelectiveColumnReader> child) {
    children_.push_back(child.get());
    childrenOwned_.push_back(std::move(child));
  }

 private:
  // Store the actual child readers.  In `children_` we only kept the raw
  // pointers and do not have ownership.
  std::vector<std::unique_ptr<SelectiveColumnReader>> childrenOwned_;
};

} // namespace bytedance::bolt::dwio::common
