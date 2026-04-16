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

#include "bolt/exec/Operator.h"
#include "bolt/exec/Spiller.h"
#include "bolt/exec/Task.h"
namespace bytedance::bolt::exec {

class VectorHasher;

// Deselects rows from 'rows' where any of the vectors managed by the 'hashers'
// has a null.
void deselectRowsWithNulls(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    SelectivityVector& rows);

// Reusable memory needed for processing filter results.
struct FilterEvalCtx {
  DecodedVector decodedResult;
  BufferPtr selectedIndices;
  BufferPtr selectedBits;

  // Make sure selectedBits has enough capacity to hold 'size' bits and return
  // raw pointer to the underlying buffer.
  uint64_t* FOLLY_NONNULL getRawSelectedBits(
      vector_size_t size,
      memory::MemoryPool* FOLLY_NONNULL pool);

  // Make sure selectedIndices buffer has enough capacity to hold 'size'
  // indices and return raw pointer to the underlying buffer.
  vector_size_t* FOLLY_NONNULL getRawSelectedIndices(
      vector_size_t size,
      memory::MemoryPool* FOLLY_NONNULL pool);
};

// Convert the results of filter evaluation as a vector of booleans into indices
// of the passing rows. Return number of rows that passed the filter. Populate
// filterEvalCtx.selectedBits and selectedIndices with the indices of the
// passing rows if only some rows pass the filter. If all or no rows passed the
// filter filterEvalCtx.selectedBits and selectedIndices are not updated.
//
// filterEvalCtx.filterResult is expected to be the vector of booleans
// representing the results of evaluating a filter.
//
// filterResult.rows are the rows in filterResult to process
vector_size_t processFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows,
    FilterEvalCtx& filterEvalCtx,
    memory::MemoryPool* FOLLY_NONNULL pool);

// Wraps the specified vector into a dictionary using the specified mapping.
// Returns vector as-is if mapping is null. An optional nulls buffer can be
// provided to introduce additional nulls.
VectorPtr wrapChild(
    vector_size_t size,
    BufferPtr mapping,
    const VectorPtr& child,
    BufferPtr nulls = nullptr);

std::vector<VectorPtr> wrapChildren(
    vector_size_t size,
    BufferPtr mapping,
    const std::vector<VectorPtr>& children,
    BufferPtr nulls = nullptr);

/// Wraps all children of the specified row vector into a dictionary using the
/// specified mapping and combines dictionary layers when possible. Returns the
/// input vector as-is if 'mapping' is null.
///
/// IMPORTANT: this helper may eagerly read and transpose 'mapping' while
/// constructing the output vectors. This is required to collapse
/// dictionary-over-dictionary inputs into a single dictionary layer and is the
/// intended fast path for callers whose row mapping is already fully
/// materialized.
///
/// As a result, callers must only use this API when every entry in 'mapping'
/// has been finalized before the call. It is not safe to pass a buffer that is
/// going to be filled incrementally after the wrapped vectors are created. In
/// such cases, any child that is already dictionary-encoded may capture a
/// partially initialized mapping and produce incorrect results.
///
/// If an operator creates output vectors first and populates row indices later,
/// it must keep a dictionary directly over the input child instead of using
/// this helper. MergeJoin::prepareOutput is one example of such a lifecycle.
RowVectorPtr wrapAndCombineDict(
    vector_size_t size,
    BufferPtr mapping,
    const RowVectorPtr& vector);

/// Projects 'inputs' according to 'identityProjection' and wraps the projected
/// children with 'mapping'.
///
/// This helper shares the same contract as wrapAndCombineDict(): the mapping
/// must already contain the final row numbers for every output position when
/// the function is called. The implementation may combine existing dictionary
/// layers immediately, which requires reading 'mapping' eagerly.
///
/// Do not use this API with a mapping buffer that will be populated after the
/// wrapped vectors are constructed. If the projected input already contains a
/// dictionary layer, delayed writes to 'mapping' will not be reflected in the
/// combined result.
void wrapIndirectChildren(
    std::vector<IdentityProjection> identityProjection,
    const std::vector<VectorPtr>& inputs,
    vector_size_t size,
    BufferPtr mapping,
    std::vector<VectorPtr>& outputs);

/// Wraps 'childVectors' into a dictionary with 'rowType' using specified
/// mapping. Returns an empty vector if mapping is null. This is different
/// than 'wrapAndCombineDict' which takes an input vector. The latter returns
/// the input vector if mapping is null.
RowVectorPtr wrapAndCombineDict(
    vector_size_t size,
    BufferPtr mapping,
    const RowTypePtr& rowType,
    const std::vector<VectorPtr>& childVectors,
    memory::MemoryPool* FOLLY_NONNULL pool);

// Ensures that all LazyVectors reachable from 'input' are loaded for all
// rows.
void loadColumns(const RowVectorPtr& input, core::ExecCtx& execCtx);

/// Scatter copy from multiple source row vectors into the target row vector.
/// 'targetIndex' is first row in 'target' to copy to. 'count' specifies how
/// many rows to copy from the sources. 'sources' and 'sourceIndices' specify
/// the source rows to copy from. If 'columnMap' is not empty, it provides the
/// column channel mappings between target row vector and source row vectors.
///
/// NOTE: all the source row vectors must have the same data type.
void gatherCopy(
    RowVector* FOLLY_NONNULL target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    const std::vector<IdentityProjection>& columnMap = {});

// gatherCopy to target from each source RowVector's sourceChannel child
void gatherCopy(
    BaseVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    column_index_t sourceChannel);

/// Generates the system-wide unique disk spill file path for an operator. It
/// will be the directory on fs with namespace support or common file prefix
/// if not. It is assumed that the disk spilling file hierarchy for an
/// operator is flat.
std::string makeOperatorSpillPath(
    const std::string& spillDir,
    int pipelineId,
    int driverId,
    int32_t operatorId);

/// Add a named runtime metric to operator 'stats'.
void addOperatorRuntimeStats(
    const std::string& name,
    const RuntimeCounter& value,
    std::unordered_map<std::string, RuntimeMetric>& stats);

/// Set a named runtime metric to operator 'stats'.
void setOperatorRuntimeStats(
    const std::string& name,
    const RuntimeCounter& value,
    std::unordered_map<std::string, RuntimeMetric>& stats);

/// Aggregates runtime metrics we want to see per operator rather than per
/// event.
void aggregateOperatorRuntimeStats(
    std::unordered_map<std::string, RuntimeMetric>& stats);

/// Allocates 'mapping' to fit at least 'size' indices and initializes them to
/// zero if 'mapping' is either: nullptr, not unique or cannot fit 'size'.
/// Returns 'mapping' as folly::Range<vector_size_t*>. Can be used by operator
/// to initialize / resize reusable state across batches of processing.
folly::Range<vector_size_t*> initializeRowNumberMapping(
    BufferPtr& mapping,
    vector_size_t size,
    memory::MemoryPool* pool);

/// Projects children of 'src' row vector according to 'projections'.
/// Optionally takes a 'mapping' and 'size' that represent the indices and
/// size, respectively, of a dictionary wrapping that should be applied to the
/// projections. The output param 'projectedChildren' will contain all the
/// final projections at the expected channel index. Indices not specified in
/// 'projections' will be left untouched in 'projectedChildren'.
void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const RowVectorPtr& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping);

/// Overload of the above function that takes reference to const vector of
/// VectorPtr as 'src' argument, instead of row vector.
void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const std::vector<VectorPtr>& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping);

RowVectorPtr wrapColumns(
    const RowVector* input,
    const std::vector<column_index_t>& channels,
    const RowTypePtr& types,
    memory::MemoryPool* pool);

} // namespace bytedance::bolt::exec
