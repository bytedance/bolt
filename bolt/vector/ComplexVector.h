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

#include <type_traits>

#include <folly/container/F14Map.h>
#include <folly/hash/Hash.h>
#include <glog/logging.h>

#include "bolt/common/memory/AllocationPool.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/LazyVector.h"
#include "bolt/vector/TypeAliases.h"
namespace bytedance::bolt {

using column_index_t = uint32_t;

constexpr column_index_t kConstantChannel =
    std::numeric_limits<column_index_t>::max();

class RowVector : public BaseVector {
 public:
  RowVector(const RowVector&) = delete;
  RowVector& operator=(const RowVector&) = delete;

  RowVector(
      bolt::memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      std::vector<VectorPtr> children,
      std::optional<vector_size_t> nullCount = std::nullopt)
      : BaseVector(
            pool,
            type,
            VectorEncoding::Simple::ROW,
            nulls,
            length,
            std::nullopt,
            nullCount,
            1),
        childrenSize_(children.size()),
        children_(std::move(children)) {
    // Some columns may not be projected out
    BOLT_CHECK_LE(children_.size(), type->size());
    [[maybe_unused]] const auto* rowType =
        dynamic_cast<const RowType*>(type.get());

    // Check child vector types.
    // This can be an expensive operation, so it's only done at debug time.
    for (auto i = 0; i < children_.size(); i++) {
      const auto& child = children_[i];
      if (child) {
        BOLT_DCHECK(
            child->type()->kindEquals(type->childAt(i)),
            "Got type {} for field `{}` at position {}, but expected {}.",
            child->type()->toString(),
            rowType->nameOf(i),
            i,
            type->childAt(i)->toString());
      }
    }
    updateContainsLazyNotLoaded();
  }

  static std::shared_ptr<RowVector> createEmpty(
      std::shared_ptr<const Type> type,
      bolt::memory::MemoryPool* pool);

  virtual ~RowVector() override {}

  bool containsNullAt(vector_size_t idx) const override;

  std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override;

  uint64_t hashValueAt(vector_size_t index) const override;

  BaseVector* loadedVector() override;

  const BaseVector* loadedVector() const override {
    return const_cast<RowVector*>(this)->loadedVector();
  }

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  /// Return the number of child vectors.
  /// This will exactly match the number of fields.
  size_t childrenSize() const {
    return childrenSize_;
  }

  // Resize a row vector by adding trailing nulls to the top level row without
  // resizing children.
  // Caller should ensure that the vector is unique before calling this method.
  void appendNulls(vector_size_t numberOfRows);

  /// Get the child vector at a given offset.
  virtual VectorPtr& childAt(column_index_t index) {
    BOLT_CHECK_LT(
        index,
        childrenSize_,
        "Trying to access non-existing child in RowVector: {}",
        toString());
    return children_[index];
  }

  virtual const VectorPtr& childAt(column_index_t index) const {
    BOLT_CHECK_LT(
        index,
        childrenSize_,
        "Trying to access non-existing child in RowVector: {}",
        toString());
    return children_[index];
  }

  /// Returns child vector for the specified field name. Throws if field with
  /// specified name doesn't exist.
  VectorPtr& childAt(const std::string& name) {
    return children_[type_->asRow().getChildIdx(name)];
  }

  const VectorPtr& childAt(const std::string& name) const {
    return children_[type_->asRow().getChildIdx(name)];
  }

  std::vector<VectorPtr>& children() {
    return children_;
  }

  const std::vector<VectorPtr>& children() const {
    return children_;
  }

  void setType(const TypePtr& type) override;

  void copy(
      const BaseVector* source,
      vector_size_t targetIndex,
      vector_size_t sourceIndex,
      vector_size_t count) override;

  void copy(
      const BaseVector* source,
      const SelectivityVector& rows,
      const vector_size_t* toSourceRow,
      bool canCopyAll) override;

  void copyRanges(
      const BaseVector* source,
      const folly::Range<const CopyRange*>& ranges) override;

  uint64_t retainedSize() const override {
    auto size = BaseVector::retainedSize();
    for (auto& child : children_) {
      if (child) {
        size += child->retainedSize();
      }
    }
    return size;
  }

  uint64_t estimateFlatSize() const override;

  uint64_t usedSize() const override {
    auto size = BaseVector::usedSize();
    for (auto& child : children_) {
      if (child) {
        size += child->usedSize();
      }
    }
    return size;
  }

  uint64_t estimateExportArrowSize() const override {
    auto size = BaseVector::estimateExportArrowSize();
    for (auto& child : children_) {
      if (child) {
        size += child->estimateExportArrowSize();
      }
    }
    return size;
  }

  using BaseVector::toString;

  std::string toString(vector_size_t index) const override;

  void ensureWritable(const SelectivityVector& rows) override;

  bool isWritable() const override;

  /// Calls BaseVector::prepareForReuse() to check and reset nulls buffer if
  /// needed, then calls BaseVector::prepareForReuse(child, 0) for all children.
  void prepareForReuse() override;

  bool mayHaveNullsRecursive() const override {
    if (BaseVector::mayHaveNullsRecursive()) {
      return true;
    }

    for (const auto& child : children_) {
      if (child->mayHaveNullsRecursive()) {
        return true;
      }
    }

    return false;
  }

  VectorPtr slice(vector_size_t offset, vector_size_t length) const override;

  bool containsLazyNotLoaded() const {
    return containsLazyNotLoaded_;
  }

  void updateContainsLazyNotLoaded() const;

  void validate(const VectorValidateOptions& options) const override;

  /// Only calls BaseVector::resize and doesn't resize the children.
  /// This function is present for backwards compatibility,
  /// until the few places that require it are migrated over.
  void unsafeResize(vector_size_t newSize, bool setNotNull = true);

  /// Resizes the parent row container and also recursively resizes the
  /// children. Note that this function will throw if the children are not
  /// uniquely referenced by the parent when increasing the size.
  /// Note : If the child is null, then it will stay null after the resize.
  virtual void resize(vector_size_t newSize, bool setNotNull = true) override;

  static bool isComposite(const std::shared_ptr<RowVector>& input);

 private:
  vector_size_t childSize() const {
    bool allConstant = false;
    for (auto& child : children_) {
      if (child) {
        if (!child->isConstantEncoding()) {
          return child->size();
        }
        allConstant = true;
      }
    }
    if (!allConstant) {
      // All are nullptr.
      return 0;
    }
    // If all children are constants which do not have a meaningful
    // size, the size is one past the last referenced child index.
    return BaseVector::length_;
  }

  void appendToChildren(
      const RowVector* source,
      vector_size_t sourceIndex,
      vector_size_t count,
      vector_size_t childSize);

  const size_t childrenSize_;

 protected:
  mutable std::vector<VectorPtr> children_;

  // Flag to indicate if any children of this vector contain lazy vector that
  // has not been loaded.  Used to optimize recursive laziness check.  This will
  // be initialized in the constructor, and should be updated by calling
  // updateContainsLazyNotLoaded whenever a new lazy child is set (e.g. in table
  // scan), or a lazy child is loaded (e.g. in LazyVector::ensureLoadedRows and
  // loadedVector).
  mutable bool containsLazyNotLoaded_;

  // Flag to indicate all children has been loaded (non-recursively).  Used to
  // optimize loadedVector calls.  If this is true, we don't recurse into
  // children to check if they are loaded.  Will be set to true when
  // loadedVector is called, and reset to false when updateContainsLazyNotLoaded
  // is called (i.e. some children are likely updated to lazy).
  mutable bool childrenLoaded_ = false;
};

// class RowInfoTracker;

class CompositeRowVector : public RowVector {
 public:
  friend class RowInfoTracker;

  struct RawCharBufferInfo {
    RawCharBufferInfo() = default;
    RawCharBufferInfo(const char* ptr, int32_t cnt)
        : ptr_(ptr), rowCount_(cnt) {}

    const char* ptr_{nullptr};
    int32_t size_{0};
    int32_t rowCount_{0};
  };

  CompositeRowVector(const CompositeRowVector&) = delete;
  CompositeRowVector& operator=(const CompositeRowVector&) = delete;

  CompositeRowVector(
      const TypePtr& type,
      vector_size_t size,
      bolt::memory::MemoryPool* pool,
      std::unique_ptr<SelectivityVector> validColumns,
      std::vector<VectorPtr>&& children)
      : RowVector(pool, type, nullptr, size, std::move(children)),
        rows_(size, memory::StlAllocator<char*>(pool)),
        rawRows_(0, memory::StlAllocator<char*>(pool)),
        validChildren_(std::move(validColumns)) {
    childrenLoaded_ = true;
    containsLazyNotLoaded_ = false;
    rowBuffer_ = std::make_unique<memory::RowBuffer>(pool);
  }

  static std::shared_ptr<CompositeRowVector> create(
      const TypePtr& type,
      vector_size_t size,
      bolt::memory::MemoryPool* pool,
      std::unique_ptr<SelectivityVector> validColumns) {
    return createInternal(type, size, pool, std::move(validColumns));
  }

  void moveto(std::shared_ptr<CompositeRowVector>& destination) {
    destination->rows_ = std::move(rows_);
    destination->rawRows_ = std::move(rawRows_);
    destination->validChildren_ = std::move(validChildren_);
    destination->rowBuffer_ = std::move(rowBuffer_);
    destination->rowToBufferMapping_ = std::move(rowToBufferMapping_);
    destination->alignment_ = alignment_;
    destination->rowOffset_ = rowOffset_;
  }

  void resize(vector_size_t newSize, bool setNotNull = true) override;

  FOLLY_ALWAYS_INLINE VectorPtr& childAt(column_index_t index) override {
    BOLT_CHECK(validChildren_ && validChildren_->isValid(index));
    return children_[index];
  }

  FOLLY_ALWAYS_INLINE const VectorPtr& childAt(
      column_index_t index) const override {
    BOLT_CHECK(validChildren_ && validChildren_->isValid(index));
    return children_[index];
  }

  void prepareForReuse() override;

  FOLLY_ALWAYS_INLINE char* newRow() {
    return rowBuffer_->current();
  }

  FOLLY_ALWAYS_INLINE char* advance(uint32_t len) {
    return rowBuffer_->advance(len);
  }

  char* allocateRows(uint32_t totalRowSize) {
    return rowBuffer_->allocate(totalRowSize);
  }

 protected:
  void markStoreStart(int32_t rowIndex, int32_t rowCount) {
    BOLT_CHECK(
        rowBuffer_->bufferCount() > 0 &&
        rowToBufferMapping_.find(rowIndex) == rowToBufferMapping_.end());
    rowToBufferMapping_[rowIndex] =
        RawCharBufferInfo(rowBuffer_->current(), rowCount);
  }

  void markStoreStop(int32_t rowIndex) {
    BOLT_CHECK(rowToBufferMapping_.find(rowIndex) != rowToBufferMapping_.end());
    rowToBufferMapping_[rowIndex].size_ =
        rowBuffer_->current() - rowToBufferMapping_[rowIndex].ptr_;
  }

 public:
  FOLLY_ALWAYS_INLINE void store(vector_size_t idx, char* row) {
    BOLT_DCHECK(idx < length_);
    rows_[idx] = row;
  }

  inline void setAlignment(int32_t alignment) {
    alignment_ = alignment;
  }

  const std::vector<char*, memory::StlAllocator<char*>>& rows() {
    return rows_;
  }

  const std::vector<char*, memory::StlAllocator<char*>>& rawRows() {
    if (rowOffset_ == 0) {
      return rows_;
    } else {
      if (rawRows_.empty()) {
        rawRows_.reserve(rows_.size());
        std::transform(
            rows_.begin(),
            rows_.end(),
            std::back_inserter(rawRows_),
            [&](char* data) { return data + rowOffset_; });
      }
      return rawRows_;
    }
  }

  inline void deepCopyAndMakeContinuous(
      char* dst,
      int32_t maxDestinationSize,
      std::vector<int32_t>& offsets) {
    BOLT_DCHECK(dst && maxDestinationSize >= rowBuffer_->allocatedBytes());
    BOLT_DCHECK(sanityCheck());
    offsets.resize(length_);
    int32_t disOffset = 0;
    for (auto const& pair : rowToBufferMapping_) {
      auto bufSize = pair.second.size_;
      simd::memcpy(dst + disOffset, pair.second.ptr_, bufSize);
      for (auto i = pair.first; i < pair.first + pair.second.rowCount_; ++i) {
        offsets[i] = disOffset + (rows_[i] - pair.second.ptr_);
      }
      disOffset += bufSize;
    }
    return;
  }

  bool sanityCheck() {
    int32_t rowCount = 0;
    for (auto it = rowToBufferMapping_.begin(); it != rowToBufferMapping_.end();
         ++it) {
      auto nextIt = std::next(it);
      if (nextIt != rowToBufferMapping_.end()) {
        BOLT_CHECK(
            it->first + it->second.rowCount_ == nextIt->first,
            "rowIndex {}, rowCount_ {}, next rowIndex {}",
            it->first,
            it->second.rowCount_,
            nextIt->first);
      }
      rowCount += it->second.rowCount_;
    }

    BOLT_CHECK(
        rowCount == length_, "rowCount {}, length_ {}", rowCount, length_);
    return true;
  }

  std::string toString(vector_size_t /*index*/) const override {
    std::stringstream oss;
    oss << " rowToBufferMapping size = " << rowToBufferMapping_.size() << ", {";
    for (auto it = rowToBufferMapping_.begin(); it != rowToBufferMapping_.end();
         ++it) {
      oss << "[" << it->first << "," << it->second.rowCount_ << ","
          << it->first + it->second.rowCount_ << "],";
    }
    oss << "}";
    return oss.str();
  }

  void setRowOffset(int32_t rowOffset) {
    rowOffset_ = rowOffset;
  }

  char* rowAt(vector_size_t idx) {
    BOLT_DCHECK(idx < length_);
    return rows_[idx];
  };

  uint64_t usedSize() const override;

  uint64_t estimateRowSize() {
    return rows_.size() > 0 ? rowBuffer_->allocatedBytes() / rows_.size() : 0;
  }

  int64_t totalRowSize() {
    return rowBuffer_->allocatedBytes();
  }

 private:
  static std::shared_ptr<CompositeRowVector> createInternal(
      const TypePtr& type,
      vector_size_t size,
      bolt::memory::MemoryPool* pool,
      std::unique_ptr<SelectivityVector> validColumns);

  // data in row format
  // first 4 bytes maybe rowSize
  std::vector<char*, memory::StlAllocator<char*>> rows_;

  // no rowSize header
  std::vector<char*, memory::StlAllocator<char*>> rawRows_;

  // columns stored in columnar format
  std::unique_ptr<SelectivityVector> validChildren_;

  // buffer that store row data
  std::unique_ptr<memory::RowBuffer> rowBuffer_;

  // map<row start index : buffer index rowBuffer_>
  std::map<int32_t, struct RawCharBufferInfo> rowToBufferMapping_;

  int32_t alignment_{1};

  int32_t rowOffset_{0};
};

class RowInfoTracker {
 public:
  explicit RowInfoTracker(
      CompositeRowVector* vector,
      int32_t startIndex,
      int32_t len)
      : startIndex_(startIndex), vector_(vector) {
    vector_->markStoreStart(startIndex, len);
  }

  ~RowInfoTracker() {
    vector_->markStoreStop(startIndex_);
  }

 private:
  int32_t startIndex_;
  CompositeRowVector* vector_;
};

// Common parent class for ARRAY and MAP vectors.  Contains 'offsets' and
// 'sizes' data and provide manipulations on them.
struct ArrayVectorBase : BaseVector {
  ArrayVectorBase(const ArrayVectorBase&) = delete;
  const BufferPtr& offsets() const {
    return offsets_;
  }

  const BufferPtr& sizes() const {
    return sizes_;
  }

  vector_size_t itemSize() const override {
    return length_ == 0 ? 0 : rawOffsets_[length_ - 1] + rawSizes_[length_ - 1];
  }

  const vector_size_t* rawOffsets() const {
    return rawOffsets_;
  }

  const vector_size_t* rawSizes() const {
    return rawSizes_;
  }

  vector_size_t offsetAt(vector_size_t index) const {
    return rawOffsets_[index];
  }

  vector_size_t sizeAt(vector_size_t index) const {
    return rawSizes_[index];
  }

  BufferPtr mutableOffsets(size_t size) {
    BaseVector::resizeIndices(length_, size, pool_, offsets_, &rawOffsets_);
    return offsets_;
  }

  BufferPtr mutableSizes(size_t size) {
    BaseVector::resizeIndices(length_, size, pool_, sizes_, &rawSizes_);
    return sizes_;
  }

  void resize(vector_size_t size, bool setNotNull = true) override {
    if (BaseVector::length_ < size) {
      BaseVector::resizeIndices(length_, size, pool_, offsets_, &rawOffsets_);
      BaseVector::resizeIndices(length_, size, pool_, sizes_, &rawSizes_);
    }
    BaseVector::resize(size, setNotNull);
  }

  // Its the caller responsibility to make sure that `offsets_` and `sizes_` are
  // safe to write at index i, i.ex not shared, or not large enough.
  void
  setOffsetAndSize(vector_size_t i, vector_size_t offset, vector_size_t size) {
    DCHECK_LT(i, BaseVector::length_);
    offsets_->asMutable<vector_size_t>()[i] = offset;
    sizes_->asMutable<vector_size_t>()[i] = size;
  }

  /// Verify that an ArrayVector/MapVector does not contain overlapping [offset,
  /// size] ranges. Throws in case overlaps are found.
  void checkRanges() const;

 protected:
  ArrayVectorBase(
      bolt::memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      VectorEncoding::Simple encoding,
      BufferPtr nulls,
      size_t length,
      std::optional<vector_size_t> nullCount,
      BufferPtr offsets,
      BufferPtr lengths)
      : BaseVector(
            pool,
            type,
            encoding,
            std::move(nulls),
            length,
            std::nullopt /*distinctValueCount*/,
            nullCount),
        offsets_(std::move(offsets)),
        rawOffsets_(offsets_->as<vector_size_t>()),
        sizes_(std::move(lengths)),
        rawSizes_(sizes_->as<vector_size_t>()) {
    BOLT_CHECK_GE(
        offsets_->capacity(), BaseVector::length_ * sizeof(vector_size_t));
    BOLT_CHECK_GE(
        sizes_->capacity(), BaseVector::length_ * sizeof(vector_size_t));
  }

  void copyRangesImpl(
      const BaseVector* source,
      const folly::Range<const CopyRange*>& ranges,
      VectorPtr* targetValues,
      VectorPtr* targetKeys);

  void validateArrayVectorBase(
      const VectorValidateOptions& options,
      vector_size_t minChildVectorSize) const;

 protected:
  BufferPtr offsets_;
  const vector_size_t* rawOffsets_;
  BufferPtr sizes_;
  const vector_size_t* rawSizes_;
};

class ArrayVector : public ArrayVectorBase {
 public:
  ArrayVector(const ArrayVector&) = delete;
  ArrayVector& operator=(const ArrayVector&) = delete;

  ArrayVector(
      bolt::memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      BufferPtr offsets,
      BufferPtr lengths,
      VectorPtr elements,
      std::optional<vector_size_t> nullCount = std::nullopt)
      : ArrayVectorBase(
            pool,
            type,
            VectorEncoding::Simple::ARRAY,
            std::move(nulls),
            length,
            nullCount,
            std::move(offsets),
            std::move(lengths)),
        elements_(BaseVector::getOrCreateEmpty(
            std::move(elements),
            type->childAt(0),
            pool)) {
    BOLT_CHECK_EQ(type->kind(), TypeKind::ARRAY);
    BOLT_CHECK(
        elements_->type()->kindEquals(type->childAt(0)),
        "Unexpected element type: {}. Expected: {}",
        elements_->type()->toString(),
        type->childAt(0)->toString());
  }

  bool containsNullAt(vector_size_t idx) const override;

  std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override;

  uint64_t hashValueAt(vector_size_t index) const override;

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  const VectorPtr& elements() const {
    return elements_;
  }

  VectorPtr& elements() {
    return elements_;
  }

  void setElements(VectorPtr elements) {
    elements_ = BaseVector::getOrCreateEmpty(
        std::move(elements), type()->childAt(0), pool_);
  }

  void setType(const TypePtr& type) override;

  void copyRanges(
      const BaseVector* source,
      const folly::Range<const CopyRange*>& ranges) override;

  uint64_t retainedSize() const override {
    return BaseVector::retainedSize() + offsets_->capacity() +
        sizes_->capacity() + elements_->retainedSize();
  }

  uint64_t estimateFlatSize() const override;

  uint64_t usedSize() const override {
    return BaseVector::usedSize() + offsets_->size() + sizes_->size() +
        elements_->usedSize();
  }

  uint64_t estimateExportArrowSize() const override {
    return BaseVector::estimateExportArrowSize() + offsets_->size() +
        sizes_->size() + elements_->estimateExportArrowSize();
  }

  using BaseVector::toString;

  std::string toString(vector_size_t index) const override;

  void ensureWritable(const SelectivityVector& rows) override;

  bool isWritable() const override;

  /// Calls BaseVector::prepareForReuse() to check and reset nulls buffer if
  /// needed, checks and resets offsets and sizes buffers, zeros out offsets and
  /// sizes if reusable, calls BaseVector::prepareForReuse(elements, 0) for the
  /// elements vector.
  virtual void prepareForReuse() override;

  bool mayHaveNullsRecursive() const override {
    return BaseVector::mayHaveNullsRecursive() ||
        elements_->mayHaveNullsRecursive();
  }

  VectorPtr slice(vector_size_t offset, vector_size_t length) const override;

  void validate(const VectorValidateOptions& options) const override;

 private:
  VectorPtr elements_;
};

class MapVector : public ArrayVectorBase {
 public:
  MapVector(const MapVector&) = delete;
  MapVector& operator=(const MapVector&) = delete;

  MapVector(
      bolt::memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      BufferPtr offsets,
      BufferPtr sizes,
      VectorPtr keys,
      VectorPtr values,
      std::optional<vector_size_t> nullCount = std::nullopt,
      bool sortedKeys = false)
      : ArrayVectorBase(
            pool,
            type,
            VectorEncoding::Simple::MAP,
            std::move(nulls),
            length,
            nullCount,
            std::move(offsets),
            std::move(sizes)),
        keys_(BaseVector::getOrCreateEmpty(
            std::move(keys),
            type->childAt(0),
            pool)),
        values_(BaseVector::getOrCreateEmpty(
            std::move(values),
            type->childAt(1),
            pool)),
        sortedKeys_{sortedKeys} {
    BOLT_CHECK_EQ(type->kind(), TypeKind::MAP);

    BOLT_CHECK(
        keys_->type()->kindEquals(type->childAt(0)),
        "Unexpected key type: {}. Expected: {}",
        keys_->type()->toString(),
        type->childAt(0)->toString());
    BOLT_CHECK(
        values_->type()->kindEquals(type->childAt(1)),
        "Unexpected value type: {}. Expected: {}",
        values_->type()->toString(),
        type->childAt(1)->toString());
  }

  virtual ~MapVector() override {}

  bool containsNullAt(vector_size_t idx) const override;

  std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override;

  uint64_t hashValueAt(vector_size_t index) const override;

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  const VectorPtr& mapKeys() const {
    return keys_;
  }

  VectorPtr& mapKeys() {
    return keys_;
  }

  const VectorPtr& mapValues() const {
    return values_;
  }

  VectorPtr& mapValues() {
    return values_;
  }

  void setType(const TypePtr& type) override;

  bool hasSortedKeys() const {
    return sortedKeys_;
  }

  void setKeysAndValues(VectorPtr keys, VectorPtr values) {
    keys_ = BaseVector::getOrCreateEmpty(
        std::move(keys), type()->childAt(0), pool_);
    values_ = BaseVector::getOrCreateEmpty(
        std::move(values), type()->childAt(1), pool_);
  }

  void copyRanges(
      const BaseVector* source,
      const folly::Range<const CopyRange*>& ranges) override;

  uint64_t retainedSize() const override {
    return BaseVector::retainedSize() + offsets_->capacity() +
        sizes_->capacity() + keys_->retainedSize() + values_->retainedSize();
  }

  uint64_t estimateFlatSize() const override;

  uint64_t usedSize() const override {
    return BaseVector::usedSize() + offsets_->size() + sizes_->size() +
        keys_->usedSize() + values_->usedSize();
  }

  uint64_t estimateExportArrowSize() const override {
    return BaseVector::estimateExportArrowSize() + offsets_->size() +
        sizes_->size() + keys_->estimateExportArrowSize() +
        values_->estimateExportArrowSize();
  }

  using BaseVector::toString;

  std::string toString(vector_size_t index) const override;

  // Sorts all maps smallest key first. This enables linear time
  // comparison and log time lookup.  This may only be done if there
  // are no other references to 'map'. Checks that 'map' is uniquely
  // referenced. This is guaranteed after construction or when
  // retrieving values from aggregation or join row containers.
  static void canonicalize(
      const std::shared_ptr<MapVector>& map,
      bool useStableSort = false);

  static void canonicalize(MapVector* map, bool useStableSort = false);

  // Returns indices into the map at 'index' such
  // that keys[indices[i]] < keys[indices[i + 1]].
  std::vector<vector_size_t> sortedKeyIndices(vector_size_t index) const;

  void ensureWritable(const SelectivityVector& rows) override;

  bool isWritable() const override;

  /// Calls BaseVector::prepareForReuse() to check and reset nulls buffer if
  /// needed, checks and resets offsets and sizes buffers, zeros out offsets and
  /// sizes if reusable, calls BaseVector::prepareForReuse(keys|values, 0) for
  /// the keys and values vectors.
  void prepareForReuse() override;

  bool mayHaveNullsRecursive() const override {
    return BaseVector::mayHaveNullsRecursive() ||
        keys_->mayHaveNullsRecursive() || values_->mayHaveNullsRecursive();
  }

  VectorPtr slice(vector_size_t offset, vector_size_t length) const override;

  void validate(const VectorValidateOptions& options) const override;

 protected:
  virtual void resetDataDependentFlags(const SelectivityVector* rows) override {
    BaseVector::resetDataDependentFlags(rows);
    sortedKeys_ = false;
  }

 private:
  // Returns true if the keys for map at 'index' are sorted from first
  // to last in the type's collation order.
  bool isSorted(vector_size_t index) const;

  // makes a Buffer with 0, 1, 2,... size-1. This is later sorted to
  // get elements in key order in each map.
  BufferPtr elementIndices() const;

  VectorPtr keys_;
  VectorPtr values_;
  bool sortedKeys_;
};

using RowVectorPtr = std::shared_ptr<RowVector>;
using ArrayVectorPtr = std::shared_ptr<ArrayVector>;
using MapVectorPtr = std::shared_ptr<MapVector>;
using CompositeRowVectorPtr = std::shared_ptr<CompositeRowVector>;

// Allocates a buffer to fit at least 'size' offsets and initializes them to
// zero.
inline BufferPtr allocateOffsets(vector_size_t size, memory::MemoryPool* pool) {
  return AlignedBuffer::allocate<vector_size_t>(size, pool, 0);
}

// Allocates a buffer to fit at least 'size' sizes and initializes them to
// zero.
inline BufferPtr allocateSizes(vector_size_t size, memory::MemoryPool* pool) {
  return AlignedBuffer::allocate<vector_size_t>(size, pool, 0);
}
} // namespace bytedance::bolt
