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

#include "bolt/dwio/common/ColumnLoader.h"
namespace bytedance::bolt::dwio::common {

// Wraps '*result' in a dictionary to make the contiguous values
// appear at the indices i 'rows'. Used when loading a LazyVector for
// a sparse set of rows in conditional exprs.
namespace {
static void
scatter(const RowSet& rows, vector_size_t resultSize, VectorPtr* result) {
  BOLT_CHECK_GE(resultSize, rows.back() + 1);

  // Initialize the indices to 0 to make the dictionary safely
  // readable also for uninitialized positions.
  auto indices =
      AlignedBuffer::allocate<vector_size_t>(resultSize, (*result)->pool(), 0);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (int32_t i = 0; i < rows.size(); ++i) {
    rawIndices[rows[i]] = i;
  }
  // Disable dictionary values caching in expression eval so that we don't need
  // to reallocate the result for every batch.
  result->get()->disableMemo();
  *result = BaseVector::wrapInDictionary(nullptr, indices, resultSize, *result);
}

bool requiresReaderCastMaterialization(
    const SelectiveColumnReader& reader,
    const ValueHook* hook) {
  return hook && reader.fileType().type()->kind() == TypeKind::BIGINT &&
      !reader.fileType().type()->isDecimal() &&
      reader.requestedType()->equivalent(*VARCHAR());
}

void applyStringHook(const BaseVector& values, ValueHook* hook) {
  auto strings = values.as<SimpleVector<StringView>>();
  for (vector_size_t i = 0; i < values.size(); ++i) {
    if (values.isNullAt(i)) {
      if (hook->acceptsNulls()) {
        hook->addNull(i);
      }
    } else {
      const auto value = strings->valueAt(i);
      hook->addValue(i, &value);
    }
  }
}
} // namespace

void ColumnLoader::loadInternal(
    RowSet rows,
    ValueHook* hook,
    vector_size_t resultSize,
    VectorPtr* result) {
  BOLT_CHECK_EQ(
      version_,
      structReader_->numReads(),
      "Loading LazyVector after the enclosing reader has moved");
  const auto offset = structReader_->lazyVectorReadOffset();
  const auto* incomingNulls = structReader_->nulls();
  const auto outputRows = structReader_->outputRows();
  raw_vector<vector_size_t> selectedRows;
  RowSet effectiveRows;
  ExceptionContextSetter exceptionContext(
      {[](BoltException::Type /*exceptionType*/, auto* reader) {
         return static_cast<SelectiveStructColumnReaderBase*>(reader)
             ->debugString();
       },
       structReader_});

  if (rows.size() == outputRows.size()) {
    // All the rows planned at creation are accessed.
    effectiveRows = outputRows;
  } else {
    // rows is a set of indices into outputRows. There has been a
    // selection between creation and loading.
    selectedRows.resize(rows.size());
    assert(!selectedRows.empty());
    for (auto i = 0; i < rows.size(); ++i) {
      selectedRows[i] = outputRows[rows[i]];
    }
    effectiveRows = RowSet(selectedRows);
  }

  structReader_->advanceFieldReader(fieldReader_, offset);
  const bool materializeForHook =
      requiresReaderCastMaterialization(*fieldReader_, hook);
  fieldReader_->scanSpec()->setValueHook(materializeForHook ? nullptr : hook);
  fieldReader_->read(offset, effectiveRows, incomingNulls);
  if (fieldReader_->fileType().type()->kind() == TypeKind::ROW) {
    // 'fieldReader_' may itself produce LazyVectors. For this it must have its
    // result row numbers set.
    static_cast<SelectiveStructColumnReaderBase*>(fieldReader_)
        ->setLoadableRows(effectiveRows);
  }
  if (!hook || materializeForHook) {
    VectorPtr materialized;
    auto* output = materializeForHook ? &materialized : result;
    fieldReader_->getValues(effectiveRows, output);
    if (materializeForHook) {
      applyStringHook(*materialized, hook);
      return;
    }
    if (((rows.back() + 1) < resultSize) || rows.size() != outputRows.size()) {
      // We read sparsely. The values that were read should appear
      // at the indices in the result vector that were given by
      // 'rows'.
      scatter(rows, resultSize, result);
    }
  }
}

} // namespace bytedance::bolt::dwio::common
