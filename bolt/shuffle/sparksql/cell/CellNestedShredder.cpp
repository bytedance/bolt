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

#include "bolt/shuffle/sparksql/cell/CellNestedShredder.h"

#include <algorithm>
#include <numeric>

#include <folly/ScopeGuard.h>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

using Kind = CellNestedLayout::Kind;

/// The null shape of the first `n` decoded rows, exactly: kNoNulls and
/// kAllNull are what the split kernels rely on.
BatchNullClass classifyNulls(DecodedVector& decoded, vector_size_t n) {
  if (!decoded.mayHaveNulls()) {
    return BatchNullClass::kNoNulls;
  }
  if (decoded.isConstantMapping()) {
    return decoded.isNullAt(0) ? BatchNullClass::kAllNull
                               : BatchNullClass::kNoNulls;
  }
  const uint64_t* nulls = decoded.nulls();
  if (nulls == nullptr || bits::isAllSet(nulls, 0, n, true)) {
    return BatchNullClass::kNoNulls;
  }
  if (bits::isAllSet(nulls, 0, n, false)) {
    return BatchNullClass::kAllNull;
  }
  return BatchNullClass::kSomeNulls;
}

/// Whether every one of the `size` decoded entries is null, whichever a
/// chunk takes: a constant null, or a flat vector all null.
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

/// Lets go of `buffer` when it was last sized past `bytes`.
void releaseOver(BufferPtr& buffer, size_t bytes) {
  if (buffer != nullptr && buffer->size() > bytes) {
    buffer.reset();
  }
}

} // namespace

CellNestedShredder::CellNestedShredder(
    const CellNestedLayout& layout,
    CellSplitter& splitter,
    uint32_t numPartitions,
    memory::MemoryPool& pool,
    uint32_t chunkPositions)
    : layout_(layout),
      splitter_(splitter),
      numPartitions_(numPartitions),
      pool_(pool),
      chunkPositions_(chunkPositions),
      inputs_(layout.numNodes()),
      decoded_(layout.numNodes()),
      nullClass_(layout.numNodes(), BatchNullClass::kAllNull),
      views_(layout.numNodes()),
      viewIndices_(layout.numNodes()),
      viewNulls_(layout.numNodes()),
      counts_(layout.numSpaces(), nullptr),
      maxCount_(layout.numSpaces(), 0),
      totals_(layout.numSpaces(), 0),
      committed_(layout.numSpaces(), 0),
      seals_(splitter.sealCount()),
      chunks_(layout.maxDepth() + 1) {
  BOLT_CHECK_GT(chunkPositions_, 0);
  const size_t numCounts =
      static_cast<size_t>(layout.numSpaces() - 1) * numPartitions_;
  auto* counts = scratch<uint32_t>(countsBuffer_, numCounts);
  std::fill_n(counts, numCounts, 0);
  for (uint32_t s = 1; s < layout.numSpaces(); ++s) {
    counts_[s] = counts + static_cast<size_t>(s - 1) * numPartitions_;
  }
  std::fill_n(scratch<uint32_t>(counters_, numPartitions_), numPartitions_, 0);
  std::fill_n(scratch<uint32_t>(ordinals_, numPartitions_), numPartitions_, 0);
  touched_.reserve(numPartitions_);
  // A wire column's null body describes the positions of its space.
  std::vector<const uint32_t*> positions;
  positions.reserve(layout.wireSpaces().size());
  for (const uint32_t space : layout.wireSpaces()) {
    positions.push_back(counts_[space]);
  }
  splitter_.setColumnPositions(std::move(positions));
}

template <typename T>
T* CellNestedShredder::scratch(BufferPtr& buffer, size_t n) {
  return reinterpret_cast<T*>(
      resetBuffer(buffer, std::max<size_t>(n, 1) * sizeof(T), &pool_));
}

template <typename T>
const BufferPtr&
CellNestedShredder::copyToScratch(BufferPtr& buffer, const T* data, size_t n) {
  std::copy_n(data, n, scratch<T>(buffer, n));
  return buffer;
}

void CellNestedShredder::followSeals() {
  if (splitter_.sealCount() == seals_) {
    return;
  }
  // Partitions with positions below the rows hold rows: a partition the
  // seal left without rows had its payload closed.
  seals_ = splitter_.sealCount();
  const uint32_t* rows = splitter_.windowRowCounts();
  for (uint32_t s = 1; s < layout_.numSpaces(); ++s) {
    uint32_t* counts = counts_[s];
    uint32_t most = 0;
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      if (rows[pid] == 0) {
        counts[pid] = 0;
      }
      most = std::max(most, counts[pid]);
    }
    maxCount_[s] = most;
  }
}

void CellNestedShredder::prepare(const RowVector& rv) {
  const uint64_t start = currentTimeNs();
  busy_ = true;
  SCOPE_EXIT {
    busy_ = false;
  };
  numRows_ = rv.size();
  std::fill(totals_.begin(), totals_.end(), 0);
  const auto& topNodes = layout_.topNodes();
  BOLT_CHECK_EQ(rv.childrenSize(), topNodes.size() + 1);
  for (uint32_t i = 0; i < topNodes.size(); ++i) {
    BOLT_CHECK(
        rv.childAt(i + 1)->type()->equivalent(*layout_.node(topNodes[i]).type),
        "Cell input schema changed");
    inputNode(topNodes[i], rv.childAt(i + 1));
  }

  // The rows' columns: a leaf as it comes, a ROW as its presence, a
  // container as its lengths.
  for (const uint32_t col : layout_.space(0).columns) {
    const auto& node = layout_.node(col);
    auto& klass = nullClass_[col];
    auto& input = inputs_[col];
    if (node.type->kind() == TypeKind::UNKNOWN || input.allNull) {
      klass = BatchNullClass::kAllNull;
      continue;
    }
    if (node.kind == Kind::kLeaf || node.kind == Kind::kRow) {
      klass = classifyNulls(decoded_[col], numRows_);
      continue;
    }
    klass = viewRows(col);
  }

  for (const uint32_t x : layout_.space(0).containers) {
    countRange(x, 0, numRows_);
  }
  shredNs_ += currentTimeNs() - start;
}

BatchNullClass CellNestedShredder::viewRows(uint32_t col) {
  auto& input = inputs_[col];
  auto& decoded = input.decoded;
  auto& view = views_[col];
  view.reset();
  if (decoded.isIdentityMapping()) {
    view = input.values;
  } else if (decoded.isConstantMapping()) {
    view = BaseVector::wrapInConstant(numRows_, decoded.index(0), input.values);
  } else {
    const BufferPtr& indices =
        copyToScratch(viewIndices_[col], decoded.indices(), numRows_);
    BufferPtr nulls;
    if (decoded.mayHaveNulls()) {
      nulls = copyToScratch(
          viewNulls_[col], decoded.nulls(), bits::nwords(numRows_));
    }
    view = BaseVector::wrapInDictionary(
        std::move(nulls), indices, numRows_, input.values);
  }
  decoded_[col].decode(*view);
  return classifyNulls(decoded_[col], numRows_);
}

void CellNestedShredder::inputNode(uint32_t n, const VectorPtr& vector) {
  const auto& node = layout_.node(n);
  auto& input = inputs_[n];
  input.vector = vector;
  input.allNull = false;
  if (node.kind == Kind::kRow) {
    inputRow(n, vector);
    return;
  }
  if (node.kind == Kind::kLeaf) {
    if (node.type->kind() != TypeKind::UNKNOWN) {
      inputColumn(n, vector);
    }
    return;
  }
  input.decoded.decode(*vector);
  // A null constant has no base container.
  const auto* base = dynamic_cast<const ArrayVectorBase*>(input.decoded.base());
  input.allNull = base == nullptr || allNull(input.decoded, vector->size());
  if (input.allNull) {
    input.values.reset();
    for (const uint32_t child : node.children) {
      clearNode(child);
    }
    return;
  }
  input.offsets = base->rawOffsets();
  input.sizes = base->rawSizes();
  input.values = std::make_shared<FlatVector<int32_t>>(
      &pool_,
      INTEGER(),
      base->nulls(),
      base->size(),
      base->sizes(),
      std::vector<BufferPtr>{});
  if (node.kind == Kind::kArray) {
    inputNode(
        node.children[0], static_cast<const ArrayVector*>(base)->elements());
  } else {
    const auto* map = static_cast<const MapVector*>(base);
    inputNode(node.children[0], map->mapKeys());
    inputNode(node.children[1], map->mapValues());
  }
}

void CellNestedShredder::inputColumn(uint32_t n, const VectorPtr& vector) {
  auto& input = inputs_[n];
  if (layout_.node(n).space == 0) {
    decoded_[n].decode(*vector);
    return;
  }
  input.decoded.decode(*vector);
  input.allNull = allNull(input.decoded, vector->size());
  // The base belongs to the vector: the view shares its ownership.
  input.values = input.allNull
      ? nullptr
      : VectorPtr(vector, const_cast<BaseVector*>(input.decoded.base()));
}

void CellNestedShredder::inputRow(uint32_t n, const VectorPtr& vector) {
  const auto& node = layout_.node(n);
  auto& input = inputs_[n];
  auto& structs = input.decoded;
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
    presence = std::make_shared<FlatVector<int16_t>>(
        &pool_, SMALLINT(), nulls, size, ones(size), std::vector<BufferPtr>{});
  }
  for (uint32_t i = 0; i < node.children.size(); ++i) {
    inputNode(node.children[i], fold(n, i, nulls));
  }
  // The structs are decoded until here, the presence from here.
  input.vector = presence;
  inputColumn(n, presence);
}

VectorPtr
CellNestedShredder::fold(uint32_t n, uint32_t i, const BufferPtr& nulls) {
  auto& input = inputs_[n];
  const auto& structs = input.decoded;
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
    return loaded;
  }
  // A field may stop short of its structs where every struct past it is
  // null: a dictionary reads nothing of it there and leaves its buffers as
  // they are.
  if (!isFlatEncoded(*loaded) || loaded->size() < size) {
    return BaseVector::wrapInDictionary(nulls, identity(size), size, loaded);
  }
  // A flat field keeps its values under its nulls and the structs'.
  BufferPtr combined = nulls;
  if (const uint64_t* own = loaded->rawNulls()) {
    const auto words = bits::nwords(size);
    auto& fieldNulls = inputs_[field].fieldNulls;
    auto* raw = scratch<uint64_t>(fieldNulls, words);
    const auto* structNulls = nulls->as<uint64_t>();
    for (size_t w = 0; w < words; ++w) {
      raw[w] = structNulls[w] & own[w];
    }
    combined = fieldNulls;
  }
  return withNulls(*loaded, std::move(combined), size, &pool_);
}

const BufferPtr& CellNestedShredder::ones(vector_size_t size) {
  if (ones_ == nullptr ||
      ones_->size() < static_cast<size_t>(size) * sizeof(int16_t)) {
    ones_ = AlignedBuffer::allocate<int16_t>(size, &pool_, int16_t{1});
  }
  return ones_;
}

const BufferPtr& CellNestedShredder::identity(vector_size_t size) {
  if (identity_ == nullptr ||
      identity_->size() < static_cast<size_t>(size) * sizeof(vector_size_t)) {
    identity_ = AlignedBuffer::allocate<vector_size_t>(size, &pool_);
    auto* indices = identity_->asMutable<vector_size_t>();
    std::iota(indices, indices + size, 0);
  }
  return identity_;
}

void CellNestedShredder::clearNode(uint32_t n) {
  auto& input = inputs_[n];
  input.vector.reset();
  input.values.reset();
  input.allNull = true;
  for (const uint32_t child : layout_.node(n).children) {
    clearNode(child);
  }
}

void CellNestedShredder::countRange(
    uint32_t x,
    vector_size_t begin,
    vector_size_t end) {
  auto& input = inputs_[x];
  if (input.allNull) {
    return;
  }
  const uint32_t t = layout_.node(x).ownedSpace;
  const auto& containers = layout_.space(t).containers;
  uint64_t& total = totals_[t];
  const auto& decoded = input.decoded;
  for (vector_size_t j = begin; j < end; ++j) {
    if (decoded.isNullAt(j)) {
      continue;
    }
    const vector_size_t i = decoded.index(j);
    const vector_size_t size = input.sizes[i];
    total += static_cast<uint64_t>(size);
    // No window carries the batch: the count stops here.
    if (total > CellNestedLayout::kMaxPositions) {
      BOLT_USER_FAIL(
          "a batch holds more than {} nested elements at one level, more "
          "than a Cell shuffle payload carries",
          CellNestedLayout::kMaxPositions);
    }
    for (const uint32_t y : containers) {
      const vector_size_t offset = input.offsets[i];
      countRange(y, offset, offset + size);
    }
  }
}

bool CellNestedShredder::fitsWindow() {
  followSeals();
  for (uint32_t t = 1; t < layout_.numSpaces(); ++t) {
    if (totals_[t] + maxCount_[t] > CellNestedLayout::kMaxPositions) {
      return false;
    }
  }
  return true;
}

void CellNestedShredder::split(
    const uint32_t* row2Partition,
    const uint32_t* partition2RowCount) {
  const uint64_t start = currentTimeNs();
  busy_ = true;
  SCOPE_EXIT {
    busy_ = false;
  };
  splitNs_ = 0;
  // A seal since prepare() - a remote push before the split began - empties
  // its partitions here. None comes before the batch ends.
  followSeals();
  std::fill(committed_.begin(), committed_.end(), 0);

  const auto& rows = layout_.space(0);
  bool someNulls = false;
  for (const uint32_t col : rows.splitColumns) {
    someNulls = someNulls || nullClass_[col] == BatchNullClass::kSomeNulls;
  }
  uint32_t* rowIndex = nullptr;
  if (someNulls) {
    rowIndex = scratch<uint32_t>(rowIndex_, numRows_);
    uint32_t* counters = counters_->asMutable<uint32_t>();
    for (vector_size_t row = 0; row < numRows_; ++row) {
      rowIndex[row] = counters[row2Partition[row]]++;
    }
    std::fill_n(counters, numPartitions_, 0);
  }
  SplitBatch batch;
  batch.decoded = decoded_.data();
  batch.row2Partition = row2Partition;
  batch.partition2RowCount = partition2RowCount;
  batch.nullClass = nullClass_.data();
  batch.numRows = numRows_;
  batch.rowIndexInPid = rowIndex;
  batch.columns = rows.splitColumns.data();
  batch.numColumns = static_cast<uint32_t>(rows.splitColumns.size());
  splitColumns(batch);

  const Entries entries{
      row2Partition, nullptr, 0, static_cast<uint32_t>(numRows_), 0};
  for (const uint32_t x : rows.containers) {
    emit(x, entries);
  }
  for (uint32_t s = 1; s < layout_.numSpaces(); ++s) {
    BOLT_CHECK_EQ(
        committed_[s],
        totals_[s],
        "positions committed in space {} differ from those counted",
        s);
  }
  // The batch's vectors go back with it, and so does the scratch of its
  // structs past the larger of its rows and a chunk.
  const size_t keep = std::max<size_t>(numRows_, chunkPositions_);
  const size_t keepNulls = bits::nwords(keep) * sizeof(uint64_t);
  for (auto& input : inputs_) {
    input.vector.reset();
    input.values.reset();
    releaseOver(input.fieldNulls, keepNulls);
    releaseOver(input.structNulls, keepNulls);
    releaseOver(input.structIndices, keep * sizeof(vector_size_t));
  }
  for (auto& view : views_) {
    view.reset();
  }
  releaseOver(ones_, keep * sizeof(int16_t));
  releaseOver(identity_, keep * sizeof(vector_size_t));
  shredNs_ += currentTimeNs() - start - splitNs_;
}

int64_t CellNestedShredder::releaseScratch() {
  if (busy_) {
    return 0;
  }
  // A prepared batch's vectors hold what they use of it.
  const int64_t held = pool_.usedBytes();
  rowIndex_.reset();
  ones_.reset();
  identity_.reset();
  for (auto& chunk : chunks_) {
    chunk.pids.reset();
    chunk.rels.reset();
    chunk.srcs.reset();
  }
  for (uint32_t col = 0; col < viewIndices_.size(); ++col) {
    viewIndices_[col].reset();
    viewNulls_[col].reset();
  }
  for (auto& input : inputs_) {
    input.fieldNulls.reset();
    input.structNulls.reset();
    input.structIndices.reset();
  }
  return held - pool_.usedBytes();
}

void CellNestedShredder::emit(uint32_t x, const Entries& parent) {
  auto& input = inputs_[x];
  if (input.allNull) {
    return;
  }
  const uint32_t t = layout_.node(x).ownedSpace;
  const auto& space = layout_.space(t);
  const auto& decoded = input.decoded;
  if (space.splitColumns.empty()) {
    // UNKNOWN elements: positions to count, no value to split.
    uint32_t* counts = counts_[t];
    for (uint32_t k = 0; k < parent.size; ++k) {
      const vector_size_t j = parent.index(k);
      if (decoded.isNullAt(j)) {
        continue;
      }
      const auto size = static_cast<uint32_t>(input.sizes[decoded.index(j)]);
      const uint32_t pid = parent.pid[k];
      counts[pid] += size;
      maxCount_[t] = std::max(maxCount_[t], counts[pid]);
      committed_[t] += size;
    }
    return;
  }

  Chunk& chunk = chunks_[parent.depth + 1];
  if (chunk.pids == nullptr) {
    scratch<uint32_t>(chunk.pids, chunkPositions_);
    scratch<uint32_t>(chunk.rels, chunkPositions_);
    scratch<vector_size_t>(chunk.srcs, chunkPositions_);
  }
  uint32_t* pids = chunk.pids->asMutable<uint32_t>();
  vector_size_t* srcs = chunk.srcs->asMutable<vector_size_t>();
  uint32_t* counters = counters_->asMutable<uint32_t>();
  chunk.size = 0;
  chunk.contiguous = true;
  for (uint32_t k = 0; k < parent.size; ++k) {
    const vector_size_t j = parent.index(k);
    if (decoded.isNullAt(j)) {
      continue;
    }
    const vector_size_t i = decoded.index(j);
    vector_size_t offset = input.offsets[i];
    auto left = static_cast<uint32_t>(input.sizes[i]);
    const uint32_t pid = parent.pid[k];
    // An entry's elements may go into several chunks.
    while (left > 0) {
      const uint32_t take = std::min(left, chunkPositions_ - chunk.size);
      std::fill_n(pids + chunk.size, take, pid);
      if (chunk.contiguous) {
        if (chunk.size == 0) {
          chunk.start = offset;
        } else if (
            offset != chunk.start + static_cast<vector_size_t>(chunk.size)) {
          std::iota(srcs, srcs + chunk.size, chunk.start);
          chunk.contiguous = false;
        }
      }
      if (!chunk.contiguous) {
        std::iota(srcs + chunk.size, srcs + chunk.size + take, offset);
      }
      if (counters[pid] == 0) {
        touched_.push_back(pid);
      }
      counters[pid] += take;
      chunk.size += take;
      offset += static_cast<vector_size_t>(take);
      left -= take;
      if (chunk.size == chunkPositions_) {
        flush(t, chunk, parent.depth + 1);
      }
    }
  }
  if (chunk.size > 0) {
    flush(t, chunk, parent.depth + 1);
  }
}

void CellNestedShredder::flush(uint32_t t, Chunk& chunk, uint32_t depth) {
  const auto& space = layout_.space(t);
  bool someNulls = false;
  for (const uint32_t col : space.splitColumns) {
    nullClass_[col] = view(col, chunk);
    someNulls = someNulls || nullClass_[col] == BatchNullClass::kSomeNulls;
  }
  const uint32_t* pids = chunk.pids->as<uint32_t>();
  uint32_t* counters = counters_->asMutable<uint32_t>();
  uint32_t* rels = nullptr;
  if (someNulls) {
    rels = chunk.rels->asMutable<uint32_t>();
    uint32_t* ordinals = ordinals_->asMutable<uint32_t>();
    const uint32_t size = chunk.size;
    if (touched_.size() == 1) {
      // One partition: an element's index in it is its chunk index.
      std::iota(rels, rels + size, 0u);
    } else {
      for (uint32_t k = 0; k < size; ++k) {
        rels[k] = ordinals[pids[k]]++;
      }
    }
    for (const uint32_t pid : touched_) {
      ordinals[pid] = 0;
    }
  }
  uint32_t* counts = counts_[t];
  SplitBatch batch;
  batch.decoded = decoded_.data();
  batch.row2Partition = pids;
  batch.partition2RowCount = counters;
  batch.nullClass = nullClass_.data();
  batch.numRows = chunk.size;
  batch.rowIndexInPid = rels;
  batch.columns = space.splitColumns.data();
  batch.numColumns = static_cast<uint32_t>(space.splitColumns.size());
  batch.positionBase = counts;
  splitColumns(batch);
  // Committed before the space's next chunk splits, which starts past them.
  for (const uint32_t pid : touched_) {
    counts[pid] += counters[pid];
    maxCount_[t] = std::max(maxCount_[t], counts[pid]);
    committed_[t] += counters[pid];
    counters[pid] = 0;
  }
  touched_.clear();

  const Entries entries{pids, chunk.sources(), chunk.start, chunk.size, depth};
  for (const uint32_t y : space.containers) {
    emit(y, entries);
  }
  chunk.size = 0;
  chunk.contiguous = true;
}

BatchNullClass CellNestedShredder::view(uint32_t col, const Chunk& chunk) {
  auto& input = inputs_[col];
  if (input.allNull) {
    return BatchNullClass::kAllNull;
  }
  auto& decoded = input.decoded;
  const auto n = static_cast<vector_size_t>(chunk.size);
  auto& view = views_[col];
  // The previous chunk's view lets go of the scratch it shares.
  view.reset();
  if (decoded.isIdentityMapping()) {
    // Elements in a row stay a flat vector; others index it.
    view = chunk.contiguous
        ? input.values->slice(chunk.start, n)
        : BaseVector::wrapInDictionary(
              BufferPtr(nullptr), chunk.srcs, n, input.values);
  } else if (decoded.isConstantMapping()) {
    view = BaseVector::wrapInConstant(n, decoded.index(0), input.values);
  } else {
    // One dictionary over the base: the wrappers' indices and nulls
    // combined.
    auto* indices = scratch<vector_size_t>(viewIndices_[col], n);
    uint64_t* nulls = decoded.mayHaveNulls()
        ? scratch<uint64_t>(viewNulls_[col], bits::nwords(n))
        : nullptr;
    const vector_size_t* src = chunk.sources();
    for (vector_size_t k = 0; k < n; ++k) {
      const vector_size_t j = src != nullptr ? src[k] : chunk.start + k;
      indices[k] = decoded.index(j);
      if (nulls != nullptr) {
        bits::setNull(nulls, k, decoded.isNullAt(j));
      }
    }
    view = BaseVector::wrapInDictionary(
        nulls != nullptr ? viewNulls_[col] : BufferPtr(nullptr),
        viewIndices_[col],
        n,
        input.values);
  }
  decoded_[col].decode(*view);
  return classifyNulls(decoded_[col], n);
}

void CellNestedShredder::splitColumns(const SplitBatch& batch) {
  const uint64_t start = currentTimeNs();
  splitter_.split(batch);
  splitNs_ += currentTimeNs() - start;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
