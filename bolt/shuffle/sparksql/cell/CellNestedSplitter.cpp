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

#include "bolt/shuffle/sparksql/cell/CellNestedSplitter.h"

#include <algorithm>
#include <numeric>

#include <folly/ScopeGuard.h>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

using Kind = CellNestedLayout::Kind;

/// Whether every one of the `size` decoded entries is null, whichever a
/// element batch takes: a constant null, or a flat vector all null.
bool allNull(DecodedVector& decoded, vector_size_t size) {
  if (!decoded.mayHaveNulls()) {
    return false;
  }
  if (decoded.isConstantMapping()) {
    return decoded.isNullAt(0);
  }
  if (!decoded.isIdentityMapping() || size == 0) {
    return false;
  }
  const uint64_t* nulls = decoded.nulls();
  return nulls != nullptr && bits::isAllSet(nulls, 0, size, false);
}

/// Whether `vector` holds its rows itself, unwrapped.
bool isFlatEncoded(const BaseVector& vector) {
  switch (vector.encoding()) {
    case VectorEncoding::Simple::FLAT:
    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP:
    case VectorEncoding::Simple::ROW:
      return true;
    default:
      return false;
  }
}

template <TypeKind kKind>
VectorPtr flatWithNulls(
    const BaseVector& vector,
    BufferPtr nulls,
    vector_size_t size,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<kKind>::NativeType;
  const auto& flat = *vector.asUnchecked<FlatVector<T>>();
  return std::make_shared<FlatVector<T>>(
      pool,
      vector.type(),
      std::move(nulls),
      size,
      flat.values(),
      std::vector<BufferPtr>(flat.stringBuffers()));
}

/// The first `size` rows of `vector`, flat encoded, under `nulls`: its
/// buffers and children shared, its nulls replaced.
VectorPtr withNulls(
    const BaseVector& vector,
    BufferPtr nulls,
    vector_size_t size,
    memory::MemoryPool* pool) {
  switch (vector.encoding()) {
    case VectorEncoding::Simple::ARRAY: {
      const auto& arrays = *vector.asUnchecked<ArrayVector>();
      return std::make_shared<ArrayVector>(
          pool,
          vector.type(),
          std::move(nulls),
          size,
          arrays.offsets(),
          arrays.sizes(),
          arrays.elements());
    }
    case VectorEncoding::Simple::MAP: {
      const auto& maps = *vector.asUnchecked<MapVector>();
      return std::make_shared<MapVector>(
          pool,
          vector.type(),
          std::move(nulls),
          size,
          maps.offsets(),
          maps.sizes(),
          maps.mapKeys(),
          maps.mapValues());
    }
    case VectorEncoding::Simple::ROW:
      return std::make_shared<RowVector>(
          pool,
          vector.type(),
          std::move(nulls),
          size,
          vector.asUnchecked<RowVector>()->children());
    default:
      return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          flatWithNulls,
          vector.typeKind(),
          vector,
          std::move(nulls),
          size,
          pool);
  }
}

/// Keep short fields under trailing null ROWs intact until foldStructNulls().
VectorPtr visiblePrefix(const VectorPtr& vector, vector_size_t size) {
  if (vector->size() <= size) {
    return vector;
  }
  const auto& loaded = BaseVector::loadedVectorShared(vector);
  if (loaded->encoding() == VectorEncoding::Simple::ROW) {
    return withNulls(*loaded, loaded->nulls(), size, loaded->pool());
  }
  return loaded->slice(0, size);
}

/// Lets go of `buffer` when it was last sized past `bytes`.
void releaseOver(BufferPtr& buffer, size_t bytes) {
  if (buffer != nullptr && buffer->size() > bytes) {
    buffer.reset();
  }
}

} // namespace

CellNestedSplitter::CellNestedSplitter(
    const CellNestedLayout& layout,
    CellSplitter& splitter,
    uint32_t numPartitions,
    memory::MemoryPool& pool)
    : layout_(layout),
      splitter_(splitter),
      numPartitions_(numPartitions),
      pool_(pool),
      preparedNodes_(layout.numNodes()),
      decoded_(layout.layout().numColumns()),
      nullClass_(layout.layout().numColumns(), NullClass::kAllNull),
      columnViews_(layout.layout().numColumns()),
      viewIndices_(layout.layout().numColumns()),
      viewNulls_(layout.layout().numColumns()),
      windowElementsPerPartition_(layout.numGroups(), nullptr),
      maxWindowElementsPerPartition_(layout.numGroups(), 0),
      batchElementCounts_(layout.numGroups(), 0),
      committedElementCounts_(layout.numGroups(), 0),
      lastSealCount_(splitter.sealCount()),
      elementBuffersByDepth_(layout.maxDepth() + 1) {
  const size_t numCounts =
      static_cast<size_t>(layout.numGroups() - 1) * numPartitions_;
  auto* counts = scratch<uint32_t>(windowElementCountsBuffer_, numCounts);
  std::fill_n(counts, numCounts, 0);
  for (uint32_t g = 1; g < layout.numGroups(); ++g) {
    windowElementsPerPartition_[g] =
        counts + static_cast<size_t>(g - 1) * numPartitions_;
  }
  std::fill_n(
      scratch<uint32_t>(blockElementsPerPartition_, numPartitions_),
      numPartitions_,
      0);
  std::fill_n(
      scratch<uint32_t>(nextIndexInPartition_, numPartitions_),
      numPartitions_,
      0);
  distinctBlockPartitionIds_.reserve(numPartitions_);
  // Output calls borrow this table while constructing null bodies. Columns
  // of a column group share its counts; row-group entries use the splitter's
  // rows.
  const auto& cellLayout = layout.layout();
  columnPositionCounts_.reserve(cellLayout.numColumns());
  for (uint32_t col = 0; col < cellLayout.numColumns(); ++col) {
    columnPositionCounts_.push_back(
        windowElementsPerPartition_[layout.nodeOfColumn(col).group]);
  }
}

template <typename T>
T* CellNestedSplitter::scratch(BufferPtr& buffer, size_t n) {
  return reinterpret_cast<T*>(
      resetBuffer(buffer, std::max<size_t>(n, 1) * sizeof(T), &pool_));
}

template <typename T>
const BufferPtr&
CellNestedSplitter::copyToScratch(BufferPtr& buffer, const T* data, size_t n) {
  std::copy_n(data, n, scratch<T>(buffer, n));
  return buffer;
}

void CellNestedSplitter::followSeals() {
  if (splitter_.sealCount() == lastSealCount_) {
    return;
  }
  // Partitions with positions below the rows hold rows: a partition the
  // seal left without rows had its payload closed.
  lastSealCount_ = splitter_.sealCount();
  const auto windowRowCounts = splitter_.windowRowCounts();
  for (uint32_t groupId = 1; groupId < layout_.numGroups(); ++groupId) {
    auto* partitionCounts = windowElementsPerPartition_[groupId];
    uint32_t maxPartitionCount = 0;
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      if (windowRowCounts[pid] == 0) {
        partitionCounts[pid] = 0;
      }
      maxPartitionCount = std::max(maxPartitionCount, partitionCounts[pid]);
    }
    maxWindowElementsPerPartition_[groupId] = maxPartitionCount;
  }
}

void CellNestedSplitter::prepare(const RowVector& rv, uint32_t columnOffset) {
  NanosecondTimer timer(&splitWallNs_);
  inPrepareOrSplit_ = true;
  SCOPE_EXIT {
    inPrepareOrSplit_ = false;
  };
  numRows_ = rv.size();
  std::fill(batchElementCounts_.begin(), batchElementCounts_.end(), 0);
  const auto& topNodes = layout_.topNodes();
  BOLT_CHECK_EQ(rv.childrenSize(), topNodes.size() + columnOffset);
  for (uint32_t i = 0; i < topNodes.size(); ++i) {
    auto child = rv.childAt(i + columnOffset);
    BOLT_DCHECK(
        child->type()->equivalent(*layout_.node(topNodes[i]).type),
        "Cell input schema changed");
    decodeNode(topNodes[i], visiblePrefix(child, numRows_));
  }

  // The rows' columns: a leaf as it comes, a ROW as its presence, a
  // container as its lengths.
  for (const uint32_t col : layout_.group(0).columns) {
    const uint32_t n = layout_.nodeIndexOfColumn(col);
    const auto& node = layout_.node(n);
    auto& klass = nullClass_[col];
    auto& input = preparedNodes_[n];
    if (input.allNull) {
      klass = NullClass::kAllNull;
      continue;
    }
    if (node.kind == Kind::kLeaf || node.kind == Kind::kRow) {
      klass = classifyNulls(decoded_[col], numRows_);
      continue;
    }
    klass = viewRows(col);
  }

  for (const auto& expansion : layout_.group(0).expansions) {
    countRange(expansion, 0, numRows_);
  }
}

NullClass CellNestedSplitter::viewRows(uint32_t col) {
  auto& input = preparedNodes_[layout_.nodeIndexOfColumn(col)];
  auto& decoded = input.sourceMapping;
  auto& view = columnViews_[col];
  view.reset();
  if (decoded.isIdentityMapping()) {
    view = input.physicalValueBase;
  } else if (decoded.isConstantMapping()) {
    view = BaseVector::wrapInConstant(
        numRows_, decoded.index(0), input.physicalValueBase);
  } else {
    const BufferPtr& indices =
        copyToScratch(viewIndices_[col], decoded.indices(), numRows_);
    BufferPtr nulls;
    if (decoded.mayHaveNulls()) {
      nulls = copyToScratch(
          viewNulls_[col], decoded.nulls(), bits::nwords(numRows_));
    }
    view = BaseVector::wrapInDictionary(
        std::move(nulls), indices, numRows_, input.physicalValueBase);
  }
  decoded_[col].decode(*view);
  return classifyNulls(decoded_[col], numRows_);
}

void CellNestedSplitter::decodeNode(uint32_t n, const VectorPtr& vector) {
  const auto& node = layout_.node(n);
  auto& input = preparedNodes_[n];
  input.source = vector;
  input.allNull = false;
  if (node.kind == Kind::kRow) {
    decodeRow(n, vector);
    return;
  }
  if (node.kind == Kind::kLeaf) {
    if (node.type->kind() != TypeKind::UNKNOWN) {
      decodeColumn(n, vector);
    }
    return;
  }
  input.sourceMapping.decode(*vector);
  // A null constant has no base container.
  const auto* base =
      dynamic_cast<const ArrayVectorBase*>(input.sourceMapping.base());
  input.allNull =
      base == nullptr || allNull(input.sourceMapping, vector->size());
  if (input.allNull) {
    input.physicalValueBase.reset();
    for (const uint32_t child : node.children) {
      clearNode(child);
    }
    return;
  }
  input.offsets = base->rawOffsets();
  input.sizes = base->rawSizes();
  input.physicalValueBase = std::make_shared<FlatVector<int32_t>>(
      &pool_,
      INTEGER(),
      base->nulls(),
      base->size(),
      base->sizes(),
      std::vector<BufferPtr>{});
  if (node.kind == Kind::kArray) {
    decodeNode(
        node.children[0], static_cast<const ArrayVector*>(base)->elements());
  } else {
    const auto* map = static_cast<const MapVector*>(base);
    decodeNode(node.children[0], map->mapKeys());
    decodeNode(node.children[1], map->mapValues());
  }
}

void CellNestedSplitter::decodeColumn(uint32_t n, const VectorPtr& vector) {
  const auto& node = layout_.node(n);
  auto& input = preparedNodes_[n];
  if (node.group == 0) {
    decoded_[node.layoutColumn].decode(*vector);
    return;
  }
  input.sourceMapping.decode(*vector);
  input.allNull = allNull(input.sourceMapping, vector->size());
  // The base belongs to the vector: the view shares its ownership.
  input.physicalValueBase = input.allNull
      ? nullptr
      : VectorPtr(vector, const_cast<BaseVector*>(input.sourceMapping.base()));
}

void CellNestedSplitter::decodeRow(uint32_t n, const VectorPtr& vector) {
  const auto& node = layout_.node(n);
  auto& input = preparedNodes_[n];
  auto& structs = input.sourceMapping;
  structs.decode(*vector);
  const vector_size_t size = vector->size();
  // A null constant has no base struct.
  const auto* base = dynamic_cast<const RowVector*>(structs.base());
  if (base == nullptr || allNull(structs, size)) {
    // Every struct null: so is every column below at these positions, and
    // no container below holds an element.
    clearNode(n);
    return;
  }
  // The structs' nulls at the entries, null when there is none; a
  // dictionary's indices for the fields to index the structs' base by.
  BufferPtr nulls;
  if (!structs.isConstantMapping()) {
    const uint64_t* raw = structs.mayHaveNulls() ? structs.nulls() : nullptr;
    if (raw != nullptr && !bits::isAllSet(raw, 0, size, true)) {
      nulls = structs.isIdentityMapping()
          ? base->nulls()
          : copyToScratch(input.structNulls, raw, bits::nwords(size));
    }
    if (!structs.isIdentityMapping()) {
      copyToScratch(input.structIndices, structs.indices(), size);
    }
  }
  VectorPtr presence;
  if (structs.isConstantMapping()) {
    presence = std::make_shared<ConstantVector<int16_t>>(
        &pool_, size, false, SMALLINT(), int16_t{1});
  } else {
    // `size` values of 1, in a buffer that grows as a batch needs.
    if (rowPresenceValues_ == nullptr ||
        rowPresenceValues_->size() <
            static_cast<size_t>(size) * sizeof(int16_t)) {
      rowPresenceValues_ =
          AlignedBuffer::allocate<int16_t>(size, &pool_, int16_t{1});
    }
    presence = std::make_shared<FlatVector<int16_t>>(
        &pool_,
        SMALLINT(),
        nulls,
        size,
        rowPresenceValues_,
        std::vector<BufferPtr>{});
  }
  for (uint32_t i = 0; i < node.children.size(); ++i) {
    decodeNode(node.children[i], foldStructNulls(n, i, nulls));
  }
  // The structs are decoded until here, the presence from here.
  input.source = presence;
  decodeColumn(n, presence);
}

VectorPtr CellNestedSplitter::foldStructNulls(
    uint32_t n,
    uint32_t i,
    const BufferPtr& nulls) {
  auto& input = preparedNodes_[n];
  const auto& structs = input.sourceMapping;
  const vector_size_t size = structs.size();
  const uint32_t field = layout_.node(n).children[i];
  const auto& vector = structs.base()->asUnchecked<RowVector>()->childAt(i);
  if (layout_.node(field).type->kind() == TypeKind::UNKNOWN) {
    // No column of it goes on the wire.
    return vector;
  }
  const auto& loaded = BaseVector::loadedVectorShared(vector);
  if (structs.isConstantMapping()) {
    return BaseVector::wrapInConstant(size, structs.index(0), loaded);
  }
  if (!structs.isIdentityMapping()) {
    return BaseVector::wrapInDictionary(
        nulls, input.structIndices, size, loaded);
  }
  if (nulls == nullptr) {
    return visiblePrefix(loaded, size);
  }
  // A field may stop short of its structs where every struct past it is
  // null: a dictionary reads nothing of it there and leaves its buffers as
  // they are.
  if (!isFlatEncoded(*loaded) || loaded->size() < size) {
    // Indices 0 to `size` - 1, in a buffer that grows as a batch needs.
    if (identityIndices_ == nullptr ||
        identityIndices_->size() <
            static_cast<size_t>(size) * sizeof(vector_size_t)) {
      identityIndices_ = AlignedBuffer::allocate<vector_size_t>(size, &pool_);
      auto* indices = identityIndices_->asMutable<vector_size_t>();
      std::iota(indices, indices + size, 0);
    }
    return BaseVector::wrapInDictionary(nulls, identityIndices_, size, loaded);
  }
  // A flat field keeps its values under its nulls and the structs'.
  BufferPtr combined = nulls;
  if (const uint64_t* own = loaded->rawNulls()) {
    const auto words = bits::nwords(size);
    auto& fieldNulls = preparedNodes_[field].fieldNulls;
    auto* raw = scratch<uint64_t>(fieldNulls, words);
    const auto* structNulls = nulls->as<uint64_t>();
    for (size_t w = 0; w < words; ++w) {
      raw[w] = structNulls[w] & own[w];
    }
    combined = fieldNulls;
  }
  return withNulls(*loaded, std::move(combined), size, &pool_);
}

void CellNestedSplitter::clearNode(uint32_t n) {
  auto& input = preparedNodes_[n];
  input.source.reset();
  input.physicalValueBase.reset();
  input.allNull = true;
  for (const uint32_t child : layout_.node(n).children) {
    clearNode(child);
  }
}

void CellNestedSplitter::countRange(
    const ContainerExpansion& expansion,
    vector_size_t begin,
    vector_size_t end) {
  auto& input = preparedNodes_[expansion.container];
  if (input.allNull) {
    return;
  }
  const auto& children = layout_.group(expansion.elementGroup).expansions;
  auto& batchElementCount = batchElementCounts_[expansion.elementGroup];
  const auto& decoded = input.sourceMapping;
  for (vector_size_t j = begin; j < end; ++j) {
    if (decoded.isNullAt(j)) {
      continue;
    }
    const vector_size_t i = decoded.index(j);
    const vector_size_t size = input.sizes[i];
    batchElementCount += static_cast<uint64_t>(size);
    if (batchElementCount > CellNestedLayout::kMaxPositions) {
      BOLT_USER_FAIL(
          "a batch holds more than {} elements in one ARRAY or MAP column, "
          "its rows together, more than a Cell shuffle payload carries",
          CellNestedLayout::kMaxPositions);
    }
    for (const auto& child : children) {
      const vector_size_t offset = input.offsets[i];
      countRange(child, offset, offset + size);
    }
  }
}

bool CellNestedSplitter::fitsWindow() {
  followSeals();
  for (uint32_t g = 1; g < layout_.numGroups(); ++g) {
    if (batchElementCounts_[g] + maxWindowElementsPerPartition_[g] >
        CellNestedLayout::kMaxPositions) {
      return false;
    }
  }
  return true;
}

void CellNestedSplitter::split(
    std::span<const uint32_t> row2Partition,
    std::span<const uint32_t> partition2RowCount) {
  BOLT_CHECK_EQ(row2Partition.size(), static_cast<size_t>(numRows_));
  BOLT_CHECK_EQ(partition2RowCount.size(), numPartitions_);
  NanosecondTimer timer(&splitWallNs_);
  inPrepareOrSplit_ = true;
  SCOPE_EXIT {
    inPrepareOrSplit_ = false;
  };
  // A seal since prepare() - a remote push before the split began - empties
  // its partitions here. None comes before the batch ends.
  followSeals();
  std::fill(committedElementCounts_.begin(), committedElementCounts_.end(), 0);

  splitGroupColumns(0, row2Partition, partition2RowCount);
  const ElementSelection rootRows{row2Partition};
  for (const auto& expansion : layout_.group(0).expansions) {
    expandContainer(expansion, rootRows);
  }
  for (uint32_t g = 1; g < layout_.numGroups(); ++g) {
    BOLT_CHECK_EQ(
        committedElementCounts_[g],
        batchElementCounts_[g],
        "positions committed in column group {} differ from those counted",
        g);
  }
  // The batch's vectors go back with it, and so does the scratch of its
  // structs past the larger of its rows and an element batch.
  const size_t keep = std::max<size_t>(numRows_, kElementBatchPositions);
  const size_t keepNulls = bits::nwords(keep) * sizeof(uint64_t);
  for (auto& input : preparedNodes_) {
    input.source.reset();
    input.physicalValueBase.reset();
    releaseOver(input.fieldNulls, keepNulls);
    releaseOver(input.structNulls, keepNulls);
    releaseOver(input.structIndices, keep * sizeof(vector_size_t));
  }
  for (auto& view : columnViews_) {
    view.reset();
  }
  releaseOver(rowPresenceValues_, keep * sizeof(int16_t));
  releaseOver(identityIndices_, keep * sizeof(vector_size_t));
}

int64_t CellNestedSplitter::releaseScratch() {
  if (inPrepareOrSplit_) {
    return 0;
  }
  // A prepared batch's vectors hold what they use of it.
  const int64_t held = pool_.usedBytes();
  indicesInPartitionBuffer_.reset();
  rowPresenceValues_.reset();
  identityIndices_.reset();
  for (auto& buffer : elementBuffersByDepth_) {
    buffer.partitionIds.reset();
    buffer.sourceIndices.reset();
  }
  for (uint32_t col = 0; col < viewIndices_.size(); ++col) {
    viewIndices_[col].reset();
    viewNulls_[col].reset();
  }
  for (auto& input : preparedNodes_) {
    input.fieldNulls.reset();
    input.structNulls.reset();
    input.structIndices.reset();
  }
  return held - pool_.usedBytes();
}

void CellNestedSplitter::ElementBlock::appendRange(
    vector_size_t offset,
    uint32_t count,
    uint32_t pid) {
  std::fill_n(partitionIds_.data() + size, count, pid);
  if (size == 0) {
    firstSourceIndex_ = offset;
  } else if (
      !sourceIndices_ &&
      offset != firstSourceIndex_ + static_cast<vector_size_t>(size)) {
    const auto indexBytes = partitionIds_.size() * sizeof(vector_size_t);
    if (!sourceIndexBuffer_ || sourceIndexBuffer_->size() < indexBytes) {
      resetBuffer(sourceIndexBuffer_, indexBytes, &pool_);
    }
    sourceIndices_ = sourceIndexBuffer_->asMutable<vector_size_t>();
    std::iota(sourceIndices_, sourceIndices_ + size, firstSourceIndex_);
  }
  if (sourceIndices_) {
    std::iota(sourceIndices_ + size, sourceIndices_ + size + count, offset);
  }
  size += count;
}

CellNestedSplitter::ElementSelection
CellNestedSplitter::ElementBlock::takeSelection() {
  ElementSelection selection{
      partitionIds_.first(size),
      firstSourceIndex_,
      sourceIndices_ ? sourceIndexBuffer_ : nullptr};
  size = 0;
  sourceIndices_ = nullptr;
  return selection;
}

void CellNestedSplitter::expandContainer(
    const ContainerExpansion& expansion,
    const ElementSelection& parents) {
  const auto& input = preparedNodes_[expansion.container];
  const auto groupId = expansion.elementGroup;
  const auto remainingElements =
      batchElementCounts_[groupId] - committedElementCounts_[groupId];
  if (remainingElements == 0) {
    return;
  }
  const auto& group = layout_.group(groupId);
  const auto& mapping = input.sourceMapping;
  if (group.columns.empty()) {
    // ARRAY/MAP of UNKNOWN needs only its per-partition position counts.
    auto* counts = windowElementsPerPartition_[groupId];
    for (uint32_t k = 0; k < parents.partitionIds.size(); ++k) {
      const auto j = parents.sourcePosition(k);
      if (!mapping.isNullAt(j)) {
        const auto count = static_cast<uint32_t>(input.sizes[mapping.index(j)]);
        const auto pid = parents.partitionIds[k];
        counts[pid] += count;
        maxWindowElementsPerPartition_[groupId] =
            std::max(maxWindowElementsPerPartition_[groupId], counts[pid]);
        committedElementCounts_[groupId] += count;
      }
    }
    return;
  }

  auto& buffer = elementBuffersByDepth_[group.depth];
  const auto blockCapacity = static_cast<uint32_t>(
      std::min<uint64_t>(remainingElements, kElementBatchPositions));
  ElementBlock block(
      {scratch<uint32_t>(buffer.partitionIds, blockCapacity), blockCapacity},
      buffer.sourceIndices,
      pool_);
  auto* blockCounts = blockElementsPerPartition_->asMutable<uint32_t>();
  for (uint32_t k = 0; k < parents.partitionIds.size(); ++k) {
    const auto j = parents.sourcePosition(k);
    if (mapping.isNullAt(j)) {
      continue;
    }
    const auto i = mapping.index(j);
    auto offset = input.offsets[i];
    auto remaining = static_cast<uint32_t>(input.sizes[i]);
    const auto pid = parents.partitionIds[k];
    while (remaining > 0) {
      const auto count = std::min(remaining, blockCapacity - block.size);
      block.appendRange(offset, count, pid);
      if (blockCounts[pid] == 0) {
        distinctBlockPartitionIds_.push_back(pid);
      }
      blockCounts[pid] += count;
      offset += count;
      remaining -= count;
      if (block.size == blockCapacity) {
        splitElementBlock(groupId, block.takeSelection());
      }
    }
  }
  if (block.size > 0) {
    splitElementBlock(groupId, block.takeSelection());
  }
}

void CellNestedSplitter::splitElementBlock(
    uint32_t groupId,
    const ElementSelection& elements) {
  const auto& group = layout_.group(groupId);
  for (const auto col : group.columns) {
    nullClass_[col] = prepareElementColumn(col, elements);
  }
  splitGroupColumns(
      groupId,
      elements.partitionIds,
      {blockElementsPerPartition_->as<uint32_t>(), numPartitions_});
  // Children reuse the per-partition block counts. They still need this
  // selection.
  commitElementCounts(groupId);
  for (const auto& expansion : group.expansions) {
    expandContainer(expansion, elements);
  }
}

void CellNestedSplitter::commitElementCounts(uint32_t groupId) {
  auto* windowCounts = windowElementsPerPartition_[groupId];
  auto* blockCounts = blockElementsPerPartition_->asMutable<uint32_t>();
  for (const uint32_t pid : distinctBlockPartitionIds_) {
    const auto count = blockCounts[pid];
    windowCounts[pid] += count;
    maxWindowElementsPerPartition_[groupId] =
        std::max(maxWindowElementsPerPartition_[groupId], windowCounts[pid]);
    committedElementCounts_[groupId] += count;
    blockCounts[pid] = 0;
  }
  distinctBlockPartitionIds_.clear();
}

std::span<const uint32_t> CellNestedSplitter::indicesInPartition(
    std::span<const uint32_t> partitionIds,
    bool rootGroup) {
  auto* indices =
      scratch<uint32_t>(indicesInPartitionBuffer_, partitionIds.size());
  if (!rootGroup && distinctBlockPartitionIds_.size() == 1) {
    std::iota(indices, indices + partitionIds.size(), 0u);
  } else {
    auto* next = nextIndexInPartition_->asMutable<uint32_t>();
    for (size_t k = 0; k < partitionIds.size(); ++k) {
      indices[k] = next[partitionIds[k]]++;
    }
    if (rootGroup) {
      std::fill_n(next, numPartitions_, 0);
    } else {
      for (const auto pid : distinctBlockPartitionIds_) {
        next[pid] = 0;
      }
    }
  }
  return {indices, partitionIds.size()};
}

NullClass CellNestedSplitter::prepareElementColumn(
    uint32_t col,
    const ElementSelection& elements) {
  auto& input = preparedNodes_[layout_.nodeIndexOfColumn(col)];
  if (input.allNull) {
    return NullClass::kAllNull;
  }
  auto& decoded = input.sourceMapping;
  const auto n = static_cast<vector_size_t>(elements.partitionIds.size());
  auto& view = columnViews_[col];
  // The previous element batch's view lets go of the scratch it shares.
  view.reset();
  if (decoded.isIdentityMapping()) {
    // Contiguous elements stay a flat vector; others index it.
    view = elements.sourceIndices == nullptr
        ? input.physicalValueBase->slice(elements.firstSourceIndex, n)
        : BaseVector::wrapInDictionary(
              BufferPtr(nullptr),
              elements.sourceIndices,
              n,
              input.physicalValueBase);
  } else if (decoded.isConstantMapping()) {
    view = BaseVector::wrapInConstant(
        n, decoded.index(0), input.physicalValueBase);
  } else {
    // One dictionary over the base: the wrappers' indices and nulls
    // combined.
    auto* indices = scratch<vector_size_t>(viewIndices_[col], n);
    uint64_t* nulls = decoded.mayHaveNulls()
        ? scratch<uint64_t>(viewNulls_[col], bits::nwords(n))
        : nullptr;
    for (vector_size_t k = 0; k < n; ++k) {
      const vector_size_t j = elements.sourcePosition(k);
      indices[k] = decoded.index(j);
      if (nulls != nullptr) {
        bits::setNull(nulls, k, decoded.isNullAt(j));
      }
    }
    view = BaseVector::wrapInDictionary(
        nulls != nullptr ? viewNulls_[col] : BufferPtr(nullptr),
        viewIndices_[col],
        n,
        input.physicalValueBase);
  }
  decoded_[col].decode(*view);
  return classifyNulls(decoded_[col], n);
}

void CellNestedSplitter::splitGroupColumns(
    uint32_t groupId,
    std::span<const uint32_t> partitionIds,
    std::span<const uint32_t> positionsPerPartition) {
  const auto& columns = layout_.group(groupId).columns;
  SplitBatch batch;
  batch.decoded = decoded_;
  batch.row2Partition = partitionIds;
  batch.partition2RowCount = positionsPerPartition;
  batch.nullClass = nullClass_;
  batch.columns = columns;
  batch.windowRowCounts = windowElementsPerPartition_[groupId];
  if (std::any_of(columns.begin(), columns.end(), [&](uint32_t col) {
        return nullClass_[col] == NullClass::kSomeNulls;
      })) {
    batch.rowIndexInPid = indicesInPartition(partitionIds, groupId == 0);
  }

  NanosecondTimer timer(&columnSplitWallNs_);
  inColumnSplit_ = true;
  SCOPE_EXIT {
    inColumnSplit_ = false;
  };
  splitter_.split(batch);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
