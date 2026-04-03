/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once
#include "bolt/type/VariantValue.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/SimpleVector.h"

namespace bytedance::bolt {

/**
 * A specialized vector for Spark 4.0 Variant type.
 * Physically, it's a struct with two fields:
 * - value: VARBINARY
 * - metadata: VARBINARY
 */
class VariantVector : public BaseVector {
 public:
  /// Standard child field names for the Spark VARIANT type.
  static constexpr const char* kValueChildName = "value";
  static constexpr const char* kMetadataChildName = "metadata";
  /// Child indices.
  static constexpr uint32_t kValueChildIndex = 0;
  static constexpr uint32_t kMetadataChildIndex = 1;

  VariantVector(
      bolt::memory::MemoryPool* pool,
      const TypePtr& type,
      BufferPtr nulls,
      size_t length,
      std::vector<VectorPtr> children,
      std::optional<vector_size_t> nullCount = std::nullopt)
      : BaseVector(
            pool,
            type,
            VectorEncoding::Simple::VARIANT,
            std::move(nulls),
            length,
            std::nullopt,
            nullCount,
            std::nullopt,
            std::nullopt),
        children_(std::move(children)) {
    BOLT_CHECK(type->isVariant());
    BOLT_CHECK_EQ(children_.size(), 2);
    BOLT_CHECK(
        children_[0]->type()->isVarbinary() ||
        children_[0]->type()->isVarchar());
    BOLT_CHECK(
        children_[1]->type()->isVarbinary() ||
        children_[1]->type()->isVarchar());
  }

  virtual ~VariantVector() override = default;

  bool isScalar() const override {
    // Not a SimpleVector. Values are stored in 2 child vectors.
    return false;
  }

  VariantValue valueAt(vector_size_t idx) const {
    const BaseVector* valVec = children_[0]->wrappedVector();
    const BaseVector* metaVec = children_[1]->wrappedVector();
    return {
        valVec->asUnchecked<SimpleVector<StringView>>()->valueAt(
            children_[0]->wrappedIndex(idx)),
        metaVec->asUnchecked<SimpleVector<StringView>>()->valueAt(
            children_[1]->wrappedIndex(idx))};
  }

  bool containsNullAt(vector_size_t idx) const override {
    return isNullAt(idx);
  }

  void resize(vector_size_t size, bool setNotNull = true) override {
    // Resize the parent first to record the requested logical length before
    // touching the child buffers. If a child resize throws (for example on
    // OOM), the parent size reflects the requested length while any children
    // that were not resized still retain their previously valid contents.
    BaseVector::resize(size, setNotNull);
    for (auto& child : children_) {
      child->resize(size, setNotNull);
    }
  }

  void unsafeResize(vector_size_t size) {
    BaseVector::resize(size);
  }

  /// WARNING: VARIANT comparison is byte-level and NOT semantically correct.
  /// The same logical value (e.g., integer 42) can have different binary
  /// representations (INT1 vs INT4 vs INT8), which will compare as unequal.
  /// This means GROUP BY, JOIN, DISTINCT, and ORDER BY on VARIANT columns
  /// may produce incorrect results. Use only for internal row-container
  /// serialization paths, such as ContainerRowSerde equality/order checks,
  /// where both sides use Bolt's exact serialized representation.
  std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override {
    if (auto result = BaseVector::compareNulls(
            isNullAt(index), other->isNullAt(otherIndex), flags)) {
      return result;
    }

    const auto* otherWrapped = other->wrappedVector();
    const auto otherWrappedIndex = other->wrappedIndex(otherIndex);
    auto otherVariant = otherWrapped->as<VariantVector>();
    BOLT_CHECK_NOT_NULL(
        otherVariant,
        "Cannot compare VARIANT vector with non-VARIANT vector. Left type: {}, right type: {}",
        type_->toString(),
        other->type()->toString());

    return valueAt(index).compare(otherVariant->valueAt(otherWrappedIndex));
  }

  /// WARNING: VARIANT hashing is byte-level. The same logical value encoded
  /// differently will produce different hashes, breaking hash-based operations
  /// like GROUP BY and JOIN. See compare() warning above.
  uint64_t hashValueAt(vector_size_t index) const override {
    if (isNullAt(index)) {
      return BaseVector::kNullHash;
    }
    return std::hash<VariantValue>{}(valueAt(index));
  }

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  VectorPtr slice(vector_size_t offset, vector_size_t length) const override {
    std::vector<VectorPtr> slicedChildren;
    for (const auto& child : children_) {
      slicedChildren.push_back(child->slice(offset, length));
    }
    return std::make_shared<VariantVector>(
        pool_,
        type_,
        sliceNulls(offset, length),
        length,
        std::move(slicedChildren));
  }

  /// Access the child vector storing variant value blobs.
  /// NOTE: This is NOT an override of BaseVector::valueVector() (which returns
  /// the inner wrapped vector for Dictionary/Constant/Sequence encodings).
  /// VariantVector is not a wrapper — it stores data in 2 child vectors.
  VectorPtr& valueChildVector() {
    return children_[0];
  }

  const VectorPtr& valueChildVector() const {
    return children_[0];
  }

  /// Access the child vector storing variant metadata blobs.
  VectorPtr& metadataChildVector() {
    return children_[1];
  }

  const VectorPtr& metadataChildVector() const {
    return children_[1];
  }

  std::vector<VectorPtr>& children() {
    return children_;
  }

  const std::vector<VectorPtr>& children() const {
    return children_;
  }

  size_t childrenSize() const {
    return children_.size();
  }

  VectorPtr& childAt(uint32_t index) {
    return children_.at(index);
  }

  const VectorPtr& childAt(uint32_t index) const {
    return children_.at(index);
  }

  static std::shared_ptr<VariantVector>
  create(bolt::memory::MemoryPool* pool, const TypePtr& type, size_t length) {
    std::vector<VectorPtr> children(2);
    children[0] = BaseVector::create(VARBINARY(), length, pool);
    children[1] = BaseVector::create(VARBINARY(), length, pool);
    return std::make_shared<VariantVector>(
        pool, type, nullptr, length, std::move(children));
  }

 private:
  std::vector<VectorPtr> children_;
};

using VariantVectorPtr = std::shared_ptr<VariantVector>;

} // namespace bytedance::bolt
