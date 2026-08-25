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

#include <memory>
#include <span>
#include <type_traits>
#include <vector>

#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec::radixsort {

class PayloadRowBatch;
class RadixSortRun;
class RadixSortRunStorage;

struct PayloadVarlenRef {
  uint64_t size;
  char* data;
};

static_assert(sizeof(PayloadVarlenRef) == 16);
static_assert(std::is_trivially_copyable_v<PayloadVarlenRef>);

struct PayloadRowColumnLayout {
  TypePtr type;
  uint64_t offset;
  uint32_t width;
  uint32_t nullByte;
  uint8_t nullMask;
  bool variable;
  bool complex;
};

class PayloadRowLayout {
 public:
  static bool supports(const Type& type);

  static std::shared_ptr<const PayloadRowLayout> create(
      const RowTypePtr& rowType);

  const RowTypePtr& rowType() const {
    return rowType_;
  }

  const std::vector<PayloadRowColumnLayout>& columns() const {
    return columns_;
  }

  uint32_t nullBytes() const {
    return nullBytes_;
  }

  bool hasVariableFields() const {
    return hasVariableFields_;
  }

  uint64_t rowWidth() const {
    return rowWidth_;
  }

 private:
  PayloadRowLayout(
      RowTypePtr rowType,
      std::vector<PayloadRowColumnLayout> columns,
      uint32_t nullBytes,
      bool hasVariableFields,
      uint64_t rowWidth)
      : rowType_(std::move(rowType)),
        columns_(std::move(columns)),
        nullBytes_(nullBytes),
        hasVariableFields_(hasVariableFields),
        rowWidth_(rowWidth) {}

  RowTypePtr rowType_;
  std::vector<PayloadRowColumnLayout> columns_;
  uint32_t nullBytes_;
  bool hasVariableFields_;
  uint64_t rowWidth_;
};

class PayloadRowReader {
 public:
  static void gather(
      const PayloadRowLayout& layout,
      std::span<char* const> rows,
      memory::MemoryPool* pool,
      RowVectorPtr& result,
      std::span<const uint8_t> mayHaveNulls = {});
};

class PayloadRowWriter {
 public:
  PayloadRowWriter();
  ~PayloadRowWriter();

  void append(
      const RowVector& input,
      RadixSortRunStorage& arena,
      PayloadRowBatch& batch);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  void clear();

  friend class RadixSortRun;
};

} // namespace bytedance::bolt::exec::radixsort
