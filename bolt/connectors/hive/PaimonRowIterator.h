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
#include "bolt/connectors/hive/SplitReader.h"
#include "bolt/connectors/hive/paimon_merge_engines/PaimonRowKind.h"
#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::connector::hive {

struct PaimonRowIterator;
using PaimonRowIteratorPtr = std::shared_ptr<PaimonRowIterator>;

struct PaimonRowIterator {
  RowVectorPtr primaryKeys;
  RowVectorPtr sequenceFields;
  // To prevent releasing underlying *valueKind
  VectorPtr valueKindVect;
  const int8_t* valueKind;
  RowVectorPtr values;
  std::vector<std::pair<RowVectorPtr, std::vector<int>>> sequenceGroups;
  vector_size_t rowIndex;
  vector_size_t length;
  SplitReader* reader;

  PaimonRowIterator()
      : PaimonRowIterator(
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            {},
            nullptr) {}

  PaimonRowIterator(
      RowVectorPtr primaryKeys_,
      RowVectorPtr sequenceFields,
      VectorPtr valueKindVect,
      const int8_t* valueKind_,
      RowVectorPtr values_,
      std::vector<std::pair<RowVectorPtr, std::vector<int>>> sequenceGroups_,
      SplitReader* reader_);

  bool pkEqual(const PaimonRowIteratorPtr other);

  int compare(PaimonRowIterator& other);

  bool next();

  bool isRetract();

  bool isAdd();
};

struct PaimonRowIteratorCompare {
  bool operator()(
      const std::shared_ptr<PaimonRowIterator>& lhs,
      const std::shared_ptr<PaimonRowIterator>& rhs) const {
    return lhs->compare(*rhs) > 0; // min-heap: smaller compare() first
  }
};

} // namespace bytedance::bolt::connector::hive
