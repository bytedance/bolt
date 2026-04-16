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

#include <unordered_map>

#include "bolt/buffer/Buffer.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/RuntimeMetrics.h"
#include "bolt/exec/OperatorUtils.h"
#include "bolt/exec/VectorHasher.h"
#include "bolt/expression/EvalCtx.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/TypeAliases.h"
#include "bolt/vector/VectorEncoding.h"
namespace bytedance::bolt::exec {

namespace {

template <typename T>
bool isInlineString(const T& value) {
  return false;
}

template <>
bool isInlineString<StringView>(const StringView& value) {
  return value.isInline();
}

template <TypeKind kind>
__attribute__((flatten)) void scalarGatherCopy(
    BaseVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    column_index_t sourceColumnChannel) {
  BOLT_DCHECK(target->isFlatEncoding());

  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = target->template asUnchecked<FlatVector<T>>();
  uint64_t* rawNulls = nullptr;
  if (std::is_same_v<T, StringView>) {
    for (int i = 0; i < count; ++i) {
      // Disabled because when gathering from RowVector, the RowVector itself
      // has no nulls, but its children may. scalarGatherCopy is still used to
      // extract child values, so sources[i] may have nulls.
      // BOLT_DCHECK(!sources[i]->mayHaveNulls());
      auto* source = sources[i]
                         ->childAt(sourceColumnChannel)
                         ->template asUnchecked<FlatVector<T>>();
      if (source->isNullAt(sourceIndices[i])) {
        if (FOLLY_UNLIKELY(rawNulls == nullptr)) {
          rawNulls = target->mutableRawNulls();
        }
        bits::setNull(rawNulls, targetIndex + i, true);
        continue;
      }
      const auto& str = source->valueAt(sourceIndices[i]);
      flatVector->setNoCopy(targetIndex + i, str);
      if (!isInlineString(str)) {
        flatVector->acquireSharedStringBuffers(source);
      }
    }
  } else {
    for (int i = 0; i < count; ++i) {
      // BOLT_DCHECK(!sources[i]->mayHaveNulls());
      auto* source = sources[i]
                         ->childAt(sourceColumnChannel)
                         ->template asUnchecked<FlatVector<T>>();
      if (source->isNullAt(sourceIndices[i])) {
        if (FOLLY_UNLIKELY(rawNulls == nullptr)) {
          rawNulls = target->mutableRawNulls();
        }
        bits::setNull(rawNulls, targetIndex + i, true);
        continue;
      }
      flatVector->set(targetIndex + i, source->valueAt(sourceIndices[i]));
    }
  }
}

void complexGatherCopy(
    BaseVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    column_index_t sourceChannel) {
  for (int i = 0; i < count; ++i) {
    target->copy(
        sources[i]->childAt(sourceChannel).get(),
        targetIndex + i,
        sourceIndices[i],
        1);
  }
}

// We want to aggregate some operator runtime metrics per operator rather than
// per event. This function returns true for such metrics.
bool shouldAggregateRuntimeMetric(const std::string& name) {
  static const folly::F14FastSet<std::string> metricNames{
      "dataSourceWallNanos",
      "dataSourceLazyWallNanos",
      "queuedWallNanos",
      "flushTimes"};
  if (metricNames.contains(name)) {
    return true;
  }

  // 'blocked*WallNanos'
  if (name.size() > 16 and strncmp(name.c_str(), "blocked", 7) == 0) {
    return true;
  }

  return false;
}

} // namespace

void deselectRowsWithNulls(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    SelectivityVector& rows) {
  bool anyChange = false;
  for (int32_t i = 0; i < hashers.size(); ++i) {
    auto& decoded = hashers[i]->decodedVector();
    if (decoded.mayHaveNulls()) {
      anyChange = true;
      const auto* nulls = hashers[i]->decodedVector().nulls(&rows);
      bits::andBits(rows.asMutableRange().bits(), nulls, 0, rows.end());
    }
  }

  if (anyChange) {
    rows.updateBounds();
  }
}

uint64_t* FilterEvalCtx::getRawSelectedBits(
    vector_size_t size,
    memory::MemoryPool* pool) {
  uint64_t* rawBits;
  BaseVector::ensureBuffer<bool, uint64_t>(size, pool, &selectedBits, &rawBits);
  return rawBits;
}

vector_size_t* FilterEvalCtx::getRawSelectedIndices(
    vector_size_t size,
    memory::MemoryPool* pool) {
  vector_size_t* rawSelected;
  BaseVector::ensureBuffer<vector_size_t>(
      size, pool, &selectedIndices, &rawSelected);
  return rawSelected;
}

namespace {
vector_size_t processConstantFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows) {
  auto constant = filterResult->as<ConstantVector<bool>>();
  if (constant->isNullAt(0) || constant->valueAt(0) == false) {
    return 0;
  }
  return rows.size();
}

vector_size_t processFlatFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows,
    FilterEvalCtx& filterEvalCtx,
    memory::MemoryPool* pool) {
  auto size = rows.size();

  auto selectedBits = filterEvalCtx.getRawSelectedBits(size, pool);
  auto nonNullBits =
      filterResult->as<FlatVector<bool>>()->rawValues<uint64_t>();
  if (filterResult->mayHaveNulls()) {
    bits::andBits(selectedBits, nonNullBits, filterResult->rawNulls(), 0, size);
  } else {
    memcpy(selectedBits, nonNullBits, bits::nbytes(size));
  }

  vector_size_t passed = 0;
  auto* rawSelected = filterEvalCtx.getRawSelectedIndices(size, pool);
  bits::forEachSetBit(
      selectedBits, 0, size, [&rawSelected, &passed](vector_size_t row) {
        rawSelected[passed++] = row;
      });
  return passed;
}

vector_size_t processEncodedFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows,
    FilterEvalCtx& filterEvalCtx,
    memory::MemoryPool* pool) {
  auto size = rows.size();

  DecodedVector& decoded = filterEvalCtx.decodedResult;
  decoded.decode(*filterResult.get(), rows);
  auto values = decoded.data<uint64_t>();
  auto nulls = decoded.nulls(&rows);
  auto indices = decoded.indices();

  vector_size_t passed = 0;
  auto* rawSelected = filterEvalCtx.getRawSelectedIndices(size, pool);
  auto* rawSelectedBits = filterEvalCtx.getRawSelectedBits(size, pool);
  memset(rawSelectedBits, 0, bits::nbytes(size));
  for (int32_t i = 0; i < size; ++i) {
    auto index = indices[i];
    if ((!nulls || !bits::isBitNull(nulls, i)) &&
        bits::isBitSet(values, index)) {
      rawSelected[passed++] = i;
      bits::setBit(rawSelectedBits, i);
    }
  }
  return passed;
}
} // namespace

vector_size_t processFilterResults(
    const VectorPtr& filterResult,
    const SelectivityVector& rows,
    FilterEvalCtx& filterEvalCtx,
    memory::MemoryPool* pool) {
  switch (filterResult->encoding()) {
    case VectorEncoding::Simple::CONSTANT:
      return processConstantFilterResults(filterResult, rows);
    case VectorEncoding::Simple::FLAT:
      return processFlatFilterResults(filterResult, rows, filterEvalCtx, pool);
    default:
      return processEncodedFilterResults(
          filterResult, rows, filterEvalCtx, pool);
  }
}

VectorPtr wrapChild(
    vector_size_t size,
    BufferPtr mapping,
    const VectorPtr& child,
    BufferPtr nulls) {
  if (!mapping) {
    return child;
  }

  return BaseVector::wrapInDictionary(nulls, mapping, size, child);
}

std::vector<VectorPtr> wrapChildren(
    vector_size_t size,
    BufferPtr mapping,
    const std::vector<VectorPtr>& children,
    BufferPtr nulls) {
  if (!mapping) {
    return children;
  }

  std::unordered_map<VectorPtr, VectorPtr> uniqueDict; // avoid duplicated wrap
  std::vector<VectorPtr> wrappedChildren(children.size());
  std::unordered_map<BufferPtr, BufferPtr> old2newMappings;
  auto curIndex = mapping->as<vector_size_t>();
  // if children[i]->valueVector()->containingLazyAndWrapped() is true, it means
  // the valueVector is isLazyNotLoaded().
  for (int i = 0; i < children.size(); ++i) {
    auto uniqueIter = uniqueDict.find(children[i]);
    if (uniqueIter != uniqueDict.end()) {
      wrappedChildren[i] = uniqueIter->second;
      continue;
    }
    if (nulls == nullptr &&
        children[i]->encoding() == VectorEncoding::Simple::DICTIONARY &&
        children[i]->rawNulls() == nullptr &&
        !children[i]->valueVector()->containingLazyAndWrapped()) {
      auto newMapping = old2newMappings.find(children[i]->wrapInfo());
      if (newMapping != old2newMappings.end()) {
        wrappedChildren[i] = wrapChild(
            size, newMapping->second, children[i]->valueVector(), nullptr);
      } else {
        // generate new mapping and wrap child.value
        auto newBuffer =
            AlignedBuffer::allocate<vector_size_t>(size, mapping->pool(), 0);
        auto newIndex = newBuffer->asMutable<vector_size_t>();
        auto childIndex = children[i]->wrapInfo()->as<vector_size_t>();
        for (auto j = 0; j < size; ++j) {
          newIndex[j] = childIndex[curIndex[j]];
        }
        wrappedChildren[i] =
            wrapChild(size, newBuffer, children[i]->valueVector(), nullptr);
        old2newMappings[children[i]->wrapInfo()] = newBuffer;
      }
    } else {
      wrappedChildren[i] = wrapChild(size, mapping, children[i]);
    }
    uniqueDict[children[i]] = wrappedChildren[i];
  }
  return wrappedChildren;
}

void wrapIndirectChildren(
    std::vector<IdentityProjection> identityProjection,
    const std::vector<VectorPtr>& inputs,
    vector_size_t size,
    BufferPtr mapping,
    std::vector<VectorPtr>& outputs) {
  std::vector<VectorPtr> columns(identityProjection.size());
  for (auto i = 0; i < identityProjection.size(); ++i) {
    BOLT_CHECK_LT(identityProjection[i].inputChannel, inputs.size());
    columns[i] = inputs[identityProjection[i].inputChannel];
  }
  auto wrapColumns = wrapChildren(size, mapping, columns);
  for (auto i = 0; i < identityProjection.size(); ++i) {
    outputs[identityProjection[i].outputChannel] = wrapColumns[i];
  }
}

RowVectorPtr wrapAndCombineDict(
    vector_size_t size,
    BufferPtr mapping,
    const RowVectorPtr& vector) {
  if (!mapping) {
    return vector;
  }
  return wrapAndCombineDict(
      size,
      std::move(mapping),
      asRowType(vector->type()),
      vector->children(),
      vector->pool());
}

RowVectorPtr wrapAndCombineDict(
    vector_size_t size,
    BufferPtr mapping,
    const RowTypePtr& rowType,
    const std::vector<VectorPtr>& childVectors,
    memory::MemoryPool* pool) {
  if (mapping == nullptr) {
    return RowVector::createEmpty(rowType, pool);
  }

  auto wrappedChildren = wrapChildren(size, mapping, childVectors, nullptr);
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, size, wrappedChildren);
}

void loadColumns(const RowVectorPtr& input, core::ExecCtx& execCtx) {
  LocalDecodedVector decodedHolder(execCtx);
  LocalSelectivityVector baseRowsHolder(&execCtx);
  LocalSelectivityVector rowsHolder(&execCtx);
  SelectivityVector* rows = nullptr;
  for (auto& child : input->children()) {
    if (isLazyNotLoaded(*child)) {
      if (!rows) {
        rows = rowsHolder.get(input->size());
        rows->setAll();
      }
      LazyVector::ensureLoadedRows(
          child,
          *rows,
          *decodedHolder.get(),
          *baseRowsHolder.get(input->size()));
    }
  }
}

void gatherCopy(
    BaseVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    column_index_t sourceChannel) {
  if (target->isScalar()) {
    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        scalarGatherCopy,
        target->type()->kind(),
        target,
        targetIndex,
        count,
        sources,
        sourceIndices,
        sourceChannel);
  } else if (target->type()->isRow()) {
    std::vector<const RowVector*> rowVectors(count);
    for (int i = 0; i < count; i++) {
      rowVectors[i] =
          sources[i]->children()[sourceChannel]->asUnchecked<RowVector>();
      target->setNull(
          targetIndex + i, rowVectors[i]->isNullAt(sourceIndices[i]));
    }
    auto result = target->as<RowVector>();
    for (int i = 0; i < result->childrenSize(); i++) {
      gatherCopy(
          result->childAt(i).get(),
          targetIndex,
          count,
          rowVectors,
          sourceIndices,
          i);
    }
  } else {
    complexGatherCopy(
        target, targetIndex, count, sources, sourceIndices, sourceChannel);
  }
}

void gatherCopy(
    RowVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    const std::vector<IdentityProjection>& columnMap) {
  BOLT_DCHECK_GE(count, 0);
  if (FOLLY_UNLIKELY(count <= 0)) {
    return;
  }
  BOLT_CHECK_LE(count, sources.size());
  BOLT_CHECK_LE(count, sourceIndices.size());
  BOLT_DCHECK_EQ(sources.size(), sourceIndices.size());
  if (!columnMap.empty()) {
    for (const auto& columnProjection : columnMap) {
      gatherCopy(
          target->childAt(columnProjection.outputChannel).get(),
          targetIndex,
          count,
          sources,
          sourceIndices,
          columnProjection.inputChannel);
    }
  } else {
    for (auto i = 0; i < target->type()->size(); ++i) {
      gatherCopy(
          target->childAt(i).get(),
          targetIndex,
          count,
          sources,
          sourceIndices,
          i);
    }
  }
}

std::string makeOperatorSpillPath(
    const std::string& spillDir,
    int pipelineId,
    int driverId,
    int32_t operatorId) {
  BOLT_CHECK(!spillDir.empty());
  return fmt::format("{}/{}_{}_{}", spillDir, pipelineId, driverId, operatorId);
}

void addOperatorRuntimeStats(
    const std::string& name,
    const RuntimeCounter& value,
    std::unordered_map<std::string, RuntimeMetric>& stats) {
  auto statIt = stats.find(name);
  if (UNLIKELY(statIt == stats.end())) {
    statIt = stats.insert(std::pair(name, RuntimeMetric(value.unit))).first;
  } else {
    BOLT_CHECK_EQ(statIt->second.unit, value.unit);
  }
  statIt->second.addValue(value.value);
}

void setOperatorRuntimeStats(
    const std::string& name,
    const RuntimeCounter& value,
    std::unordered_map<std::string, RuntimeMetric>& stats) {
  if (UNLIKELY(stats.count(name) == 0)) {
    stats.insert(std::pair(name, RuntimeMetric(value.unit)));
  } else {
    stats[name] = RuntimeMetric(value.unit);
  }
  stats.at(name).addValue(value.value);
}

void aggregateOperatorRuntimeStats(
    std::unordered_map<std::string, RuntimeMetric>& stats) {
  for (auto& runtimeMetric : stats) {
    if (shouldAggregateRuntimeMetric(runtimeMetric.first)) {
      runtimeMetric.second.aggregate();
    }
  }
}

folly::Range<vector_size_t*> initializeRowNumberMapping(
    BufferPtr& mapping,
    vector_size_t size,
    memory::MemoryPool* pool) {
  if (!mapping || !mapping->unique() ||
      mapping->size() < sizeof(vector_size_t) * size) {
    mapping = allocateIndices(size, pool);
  }
  return folly::Range(mapping->asMutable<vector_size_t>(), size);
}

void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const RowVectorPtr& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping) {
  projectChildren(
      projectedChildren, src->children(), projections, size, mapping);
}

void projectChildren(
    std::vector<VectorPtr>& projectedChildren,
    const std::vector<VectorPtr>& src,
    const std::vector<IdentityProjection>& projections,
    int32_t size,
    const BufferPtr& mapping) {
  for (auto [inputChannel, outputChannel] : projections) {
    if (outputChannel >= projectedChildren.size()) {
      projectedChildren.resize(outputChannel + 1);
    }
  }
  wrapIndirectChildren(projections, src, size, mapping, projectedChildren);
}

//// adjust SpillCompressionKind if estimatedSpillSize too large
void adjustSpillCompressionKind(
    uint64_t totalRowCount,
    uint64_t processedRowCount,
    uint64_t currentUsage,
    uint64_t lowCompressSize,
    uint64_t highCompressSize,
    common::SpillConfig*& spillConf) {
  if (!spillConf ||
      spillConf->compressionKind !=
          common::CompressionKind::CompressionKind_NONE) {
    LOG(INFO) << "compressionKind is NOT NONE, use setting Kind:"
              << spillConf->compressionKind;
    return;
  }

  if (totalRowCount <= 0 || processedRowCount <= 0 || currentUsage <= 0) {
    LOG(INFO) << "DO NOTHING!!!  totalRowCount=" << totalRowCount
              << ", processedRowCount=" << processedRowCount
              << ", currentUsage=" << currentUsage;
    return;
  }

  auto estimatedSpillSize =
      currentUsage * totalRowCount / (processedRowCount + 1);

  if (estimatedSpillSize >= highCompressSize) {
    spillConf->compressionKind = common::CompressionKind::CompressionKind_ZSTD;
    LOG(INFO) << "Spill compressionKind changed, from NONE to ZSTD"
              << ", estimatedSpillSize:" << estimatedSpillSize
              << ", highCompressSize:" << highCompressSize
              << ", currentUsage:" << currentUsage
              << ", processedRowCount:" << processedRowCount
              << ", totalRowCount:" << totalRowCount;
  } else if (estimatedSpillSize >= lowCompressSize) {
    spillConf->compressionKind = common::CompressionKind::CompressionKind_LZ4;
    LOG(INFO) << "Spill compressionKind changed, from NONE to LZ4"
              << ", estimatedSpillSize:" << estimatedSpillSize
              << ", lowCompressSize:" << lowCompressSize
              << ", currentUsage:" << currentUsage
              << ", processedRowCount:" << processedRowCount
              << ", totalRowCount:" << totalRowCount;
  }
}

RowVectorPtr wrapColumns(
    const RowVector* input,
    const std::vector<column_index_t>& channels,
    const RowTypePtr& types,
    memory::MemoryPool* pool) {
  BOLT_CHECK_NOT_NULL(input);

  std::vector<VectorPtr> children;
  children.reserve(channels.size());
  for (auto& channel : channels)
    children.push_back(input->childAt(channel));

  // Preserve row-level nulls if present on the input RowVector; otherwise
  // nullptr.
  BufferPtr rowNulls = input->nulls();

  return std::make_shared<RowVector>(
      pool, types, rowNulls, input->size(), children);
}

} // namespace bytedance::bolt::exec
