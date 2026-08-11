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

#include <cstdint>

#include <folly/Range.h>

#include "bolt/vector/BaseVector.h"
#include "bolt/vector/DecodedVector.h"

namespace bytedance::bolt::row::dense_row {
struct RowCursor;
}

// Scalar column fast path: a scalar-typed (non ARRAY/MAP/ROW) top-level column
// has a trivial per-row wire layout `[v]` — no row marker, no cardinality
// cards, no parent-null filtering — so it is encoded/decoded column-at-a-time,
// skipping the SlotView machinery entirely. DenseRow routes each scalar
// top-level field here and each complex field to the general path
// (DenseRowGeneral.h).
namespace bytedance::bolt::row::dense_row::scalar {

// Column-at-a-time size accumulation: adds field `dec`'s per-row byte counts
// into rowSizes[0..N).
void addColumnSizes(
    const Type& type,
    const DecodedVector& dec,
    vector_size_t N,
    size_t* rowSizes);

// Column-at-a-time write: appends field `dec`'s bytes through per-row cursors
// rowCursors[0..N), advancing each.
void writeColumn(
    const Type& type,
    const DecodedVector& dec,
    vector_size_t N,
    uint8_t** rowCursors);

void writeColumn(
    const Type& type,
    const DecodedVector& dec,
    vector_size_t offset,
    vector_size_t N,
    uint8_t** rowCursors);

// Column-at-a-time read: decodes one scalar value per row from cursors[0..N)
// into `dst`, advancing each cursor. Inverse of writeColumn
void readColumn(
    const Type& type,
    BaseVector& dst,
    vector_size_t N,
    folly::Range<dense_row::RowCursor*> cursors);

} // namespace bytedance::bolt::row::dense_row::scalar
