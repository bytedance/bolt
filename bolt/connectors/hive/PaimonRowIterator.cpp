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

#include "bolt/connectors/hive/PaimonRowIterator.h"
namespace bytedance::bolt::connector::hive {

PaimonRowIterator::PaimonRowIterator(
    RowVectorPtr primaryKeys_,
    RowVectorPtr sequenceFields_,
    VectorPtr valueKindVect_,
    const int8_t* valueKind_,
    RowVectorPtr values_,
    std::vector<std::pair<RowVectorPtr, std::vector<int>>> sequenceGroups_,
    SplitReader* reader_)
    : primaryKeys(std::move(primaryKeys_)),
      sequenceFields(std::move(sequenceFields_)),
      valueKindVect(valueKindVect_),
      valueKind(valueKind_),
      values(std::move(values_)),
      sequenceGroups(std::move(sequenceGroups_)),
      reader(reader_),
      rowIndex(0),
      length(values ? values->size() : 0) {
  if (length > 0) {
    BOLT_CHECK_EQ(
        primaryKeys->size(),
        length,
        "Paimon iterator has mismatched length:primaryKeys");
    BOLT_CHECK_EQ(
        sequenceFields->size(),
        length,
        "Paimon iterator has mismatched length:sequenceFields");
    BOLT_CHECK_EQ(
        valueKindVect->size(),
        length,
        "Paimon iterator has mismatched length:valueKindVect");
    BOLT_CHECK_EQ(
        values->size(), length, "Paimon iterator has mismatched length:values");
  }
}

bool PaimonRowIterator::pkEqual(const PaimonRowIteratorPtr other) {
  if (!primaryKeys)
    return false;

  return primaryKeys->equalValueAt(
      other->primaryKeys.get(), rowIndex, other->rowIndex);
}

int PaimonRowIterator::compare(PaimonRowIterator& other) {
  int pkComparison =
      primaryKeys
          ->compare(
              other.primaryKeys.get(), rowIndex, other.rowIndex, CompareFlags())
          .value();
  if (pkComparison)
    return pkComparison;

  return sequenceFields
      ->compare(
          other.sequenceFields.get(), rowIndex, other.rowIndex, CompareFlags())
      .value();
}

bool PaimonRowIterator::next() {
  if (++rowIndex < length) {
    return true;
  }

  return false;
}

bool PaimonRowIterator::isRetract() {
  PaimonRowKind rowKind = static_cast<PaimonRowKind>(valueKind[rowIndex]);
  return hive::isRetract(rowKind);
}

bool PaimonRowIterator::isAdd() {
  PaimonRowKind rowKind = static_cast<PaimonRowKind>(valueKind[rowIndex]);
  return hive::isAdd(rowKind);
}

} // namespace bytedance::bolt::connector::hive
