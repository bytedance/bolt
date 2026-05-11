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
#include "bolt/vector/LazyComplexVector.h"

#include <fmt/format.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/vector/LazyComplexCodec.h"

namespace bytedance::bolt {

LazyComplexVector::LazyComplexVector(
    memory::MemoryPool* pool,
    TypePtr originalType,
    std::shared_ptr<FlatVector<StringView>> bytes)
    : BaseVector(
          pool,
          originalType,
          VectorEncoding::Simple::LAZY_COMPLEX,
          bytes->nulls(),
          bytes->size()),
      originalType_(std::move(originalType)),
      bytes_(std::move(bytes)) {}

std::string LazyComplexVector::toString(vector_size_t index) const {
  if (isNullAt(index)) {
    return "null";
  }
  return fmt::format("<lazy {} bytes>", bytes_->valueAt(index).size());
}

std::optional<int32_t> LazyComplexVector::compare(
    const BaseVector* /*other*/,
    vector_size_t /*index*/,
    vector_size_t /*otherIndex*/,
    CompareFlags /*flags*/) const {
  BOLT_FAIL("compare() not supported for LAZY_COMPLEX; call decode() first");
}

uint64_t LazyComplexVector::hashValueAt(vector_size_t /*index*/) const {
  BOLT_FAIL(
      "hashValueAt() not supported for LAZY_COMPLEX; call decode() first");
}

void LazyComplexVector::copyRanges(
    const BaseVector* source,
    const folly::Range<const CopyRange*>& ranges) {
  BOLT_CHECK(
      source->encoding() == VectorEncoding::Simple::LAZY_COMPLEX,
      "LazyComplexVector::copyRanges requires a LAZY_COMPLEX source; encodeToLazy first");
  const auto* lazySource = static_cast<const LazyComplexVector*>(source);
  BOLT_CHECK(
      type()->equivalent(*lazySource->type()),
      "LazyComplexVector::copyRanges requires matching original types");
  bytes_->copyRanges(lazySource->encoded().get(), ranges);
}

VectorPtr LazyComplexVector::slice(vector_size_t offset, vector_size_t length)
    const {
  auto slicedBytes = std::dynamic_pointer_cast<FlatVector<StringView>>(
      bytes_->slice(offset, length));
  BOLT_CHECK_NOT_NULL(slicedBytes);
  return std::make_shared<LazyComplexVector>(pool_, originalType_, slicedBytes);
}

void LazyComplexVector::resize(vector_size_t newSize, bool setNotNull) {
  bytes_->resize(newSize, setNotNull);
  BaseVector::length_ = newSize;
  BaseVector::nulls_ = bytes_->nulls();
  BaseVector::rawNulls_ =
      BaseVector::nulls_ ? BaseVector::nulls_->as<uint64_t>() : nullptr;
}

void LazyComplexVector::prepareForReuse() {
  // Delegate the actual reset to the inner FlatVector<StringView>: it clears
  // stale StringViews, drops the prior batch's encoded-bytes arena
  // (stringBuffers_), and reuses the values buffer when mutable. Then mirror
  // the cleaned nulls back into the wrapper so isNullAt/rawNulls() stay in
  // sync — the wrapper's BaseVector state shadows bytes_.
  bytes_->prepareForReuse();
  BaseVector::nulls_ = bytes_->nulls();
  BaseVector::rawNulls_ =
      BaseVector::nulls_ ? BaseVector::nulls_->as<uint64_t>() : nullptr;
  resetDataDependentFlags(nullptr);
}

VectorPtr LazyComplexVector::decode(
    const SelectivityVector& rows,
    memory::MemoryPool* pool) const {
  const auto* codec = LazyComplexCodec::activeCodec();
  BOLT_CHECK_NOT_NULL(
      codec,
      "LazyComplexVector::decode() called but no active codec; call LazyComplexCodec::setActiveFormat() first");
  return codec->decode(*this, rows, pool);
}

} // namespace bytedance::bolt
