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

#include "bolt/exec/radixsort/RadixSortRunStorage.h"

namespace bytedance::bolt::exec::radixsort {

class PayloadRowSizes {
 public:
  vector_size_t size() const {
    return size_;
  }

  uint64_t fixedBytes() const {
    return fixedBytes_;
  }

  uint64_t heapBytes() const {
    return heapBytes_;
  }

  uint64_t heapSizeAt(vector_size_t row) const;

 private:
  friend class PayloadRowWriter;

  vector_size_t size_{0};
  uint64_t fixedBytes_{0};
  uint64_t heapBytes_{0};
  BufferPtr heapSizes_;
};

class PayloadRowWriter {
 public:
  static void measure(
      const RowVector& input,
      const PayloadRowLayout& layout,
      memory::MemoryPool* pool,
      PayloadRowSizes& sizes);

  static void append(
      const RowVector& input,
      RadixSortRunStorage& arena,
      PayloadRowBatch& batch);

  static void appendFixedOnly(
      const RowVector& input,
      RadixSortRunStorage& arena,
      PayloadRowBatch& batch);

  static void append(
      const RowVector& input,
      RadixSortRunStorage& arena,
      const PayloadRowSizes& sizes,
      PayloadRowBatch& batch);
};

} // namespace bytedance::bolt::exec::radixsort
