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

#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorEncoding.h"

namespace bytedance::bolt {

class LazyComplexVector : public BaseVector {
 public:
  LazyComplexVector(
      memory::MemoryPool* pool,
      TypePtr originalType,
      std::shared_ptr<FlatVector<StringView>> bytes);

  std::string toString(vector_size_t index) const override;

  std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override;

  uint64_t hashValueAt(vector_size_t index) const override;

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override {
    BOLT_FAIL("hashAll() not supported for LAZY_COMPLEX; call decode() first");
  }

  bool containsNullAt(vector_size_t idx) const override {
    return isNullAt(idx);
  }

  void copyRanges(
      const BaseVector* source,
      const folly::Range<const CopyRange*>& ranges) override;

  VectorPtr slice(vector_size_t offset, vector_size_t length) const override;

  void resize(vector_size_t newSize, bool setNotNull = true) override;

  void prepareForReuse() override;

  // Lazy-specific API.
  StringView valueAt(vector_size_t index) const {
    return bytes_->valueAt(index);
  }
  const std::shared_ptr<FlatVector<StringView>>& encoded() const {
    return bytes_;
  }

  VectorPtr decode(const SelectivityVector& rows, memory::MemoryPool* pool)
      const;

 private:
  const TypePtr originalType_;
  std::shared_ptr<FlatVector<StringView>> bytes_;
};

using LazyComplexVectorPtr = std::shared_ptr<LazyComplexVector>;

} // namespace bytedance::bolt
