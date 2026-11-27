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

#include "bolt/shuffle/sparksql/cell/CellNestedAssembler.h"

#include <algorithm>
#include <cstring>

#include "bolt/common/base/Exceptions.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

using Kind = CellNestedLayout::Kind;

} // namespace

CellNestedAssembler::CellNestedAssembler(
    RowTypePtr type,
    ShuffleDecompressor* decompressor,
    memory::MemoryPool* pool)
    : layout_(std::move(type)),
      pool_(pool),
      payloadDecoder_(layout_.layout(), decompressor, pool),
      lengthDecoders_(layout_.numGroups()),
      payloadLengths_(layout_.numGroups()),
      payloadGroupRowCounts_(layout_.numGroups(), 0),
      containers_(layout_.numGroups()) {
  for (uint32_t stream = 0; stream < layout_.layout().numStreams(); ++stream) {
    if (layout_.layout().stream(stream).kind == StreamKind::kStringData) {
      stringDataStreams_.push_back(stream);
    }
  }
  // The column groups' decoders open what payloadDecoder_ decompressed.
  for (uint32_t g = 0; g < layout_.numGroups(); ++g) {
    const auto& group = layout_.group(g);
    decoders_.push_back(std::make_unique<CellPayloadDecoder>(
        payloadDecoder_, group.valueColumns));
    if (!group.expansions.empty()) {
      lengthDecoders_[g] = std::make_unique<CellPayloadDecoder>(
          payloadDecoder_, group.lengthColumns);
    }
  }
}

void CellNestedAssembler::openPayload(ShuffleByteSource& in) {
  const uint32_t rootRowCount = payloadDecoder_.readPayloadStreams(in);
  payloadGroupRowCounts_[0] = rootRowCount;
  payloadFitsBatch_ = true;
  uint64_t bytes = 0;
  // Parent groups precede their children, so their lengths give each child
  // group's row count before its columns open.
  for (uint32_t g = 0; g < layout_.numGroups(); ++g) {
    const auto& group = layout_.group(g);
    const auto groupRowCount = payloadGroupRowCounts_[g];
    bytes += uint64_t{groupRowCount} *
        (decoders_[g]->rowBytes() + group.expansions.size() * sizeof(int32_t));
    decoders_[g]->openProjectedColumns(groupRowCount);
    if (group.expansions.empty()) {
      continue;
    }
    auto& lengths = payloadLengths_[g];
    lengths.reset();
    auto& lengthDecoder = *lengthDecoders_[g];
    lengthDecoder.openProjectedColumns(groupRowCount);
    lengths = lengthDecoder.finishBatch(/*appendRemainingRows=*/true);
    for (uint32_t col = 0; col < group.expansions.size(); ++col) {
      const auto elementGroup = group.expansions[col].elementGroup;
      const auto& column = lengths->children()[col];
      uint64_t elements = 0;
      if (column->isFlatEncoding()) {
        const auto* flat = column->asUnchecked<FlatVector<int32_t>>();
        int32_t minLength = 0;
        for (const auto length : std::span(flat->rawValues(), flat->size())) {
          minLength = std::min(minLength, length);
          elements += static_cast<uint32_t>(length);
        }
        if (minLength < 0) {
          failMalformedPayload("negative container length");
        }
      }
      if (elements + decoders_[elementGroup]->batchRows() >
          CellNestedLayout::kMaxPositions) {
        if (elements > CellNestedLayout::kMaxPositions) {
          failMalformedPayload("container lengths beyond the position limit");
        }
        payloadFitsBatch_ = false;
      }
      payloadGroupRowCounts_[elementGroup] = static_cast<uint32_t>(elements);
    }
  }
  for (const auto stream : stringDataStreams_) {
    bytes += payloadDecoder_.streamBytes(stream).size();
  }
  payloadRowBytes_ = std::max<uint64_t>(1, bytes / std::max(1u, rootRowCount));
}

RowVectorPtr CellNestedAssembler::nextBatch(
    vector_size_t maxBatchRows,
    int64_t byteBudget,
    const std::function<bool()>& nextPayload) {
  BOLT_CHECK_GT(byteBudget, 0, "byteBudget must be positive");
  const uint64_t budget = static_cast<uint64_t>(byteBudget);
  auto& rows = *decoders_[0];
  uint64_t batchBytes = 0;
  while (true) {
    const vector_size_t batchRows = rows.batchRows();
    if (batchRows >= maxBatchRows) {
      break;
    }
    // Stop once an open batch reaches the byte budget.
    if (batchRows > 0 && batchBytes >= budget) {
      break;
    }
    if (rows.payloadRowsLeft() == 0) {
      if (!nextPayload()) {
        // End of input, or a composite block that comes out on its own.
        break;
      }
      if (batchRows > 0 && !payloadFitsBatch_) {
        // The new payload starts the next batch, whose positions always
        // take it.
        break;
      }
      continue;
    }
    // The rows the budget has room for, by the payload's bytes per row.
    const uint32_t rowsToAppend = static_cast<uint32_t>(std::min<uint64_t>(
        std::min(
            rows.payloadRowsLeft(),
            static_cast<uint32_t>(maxBatchRows - batchRows)),
        std::max<uint64_t>(1, (budget - batchBytes) / payloadRowBytes_)));
    batchBytes += appendGroup(0, rowsToAppend);
  }
  return rows.batchRows() == 0 ? nullptr : assemble();
}

uint64_t CellNestedAssembler::appendGroup(uint32_t groupId, uint32_t numRows) {
  auto& decoder = *decoders_[groupId];
  const auto& group = layout_.group(groupId);
  const auto batchOffset = decoder.batchRows();
  const vector_size_t payloadOffset =
      payloadGroupRowCounts_[groupId] - decoder.payloadRowsLeft();
  const auto previousBytes = decoder.batchBytes();
  decoder.appendRows(numRows);
  uint64_t bytes = decoder.batchBytes() - previousBytes +
      uint64_t{numRows} * group.expansions.size() * sizeof(int32_t);

  for (size_t col = 0; col < group.expansions.size(); ++col) {
    const auto& lengths = payloadLengths_[groupId]->children()[col];
    const auto elementGroup = group.expansions[col].elementGroup;
    auto& container = containers_[elementGroup];
    const auto& elements = *decoders_[elementGroup];
    container.appendLengths(lengths, payloadOffset, batchOffset, numRows);
    auto* entryOffsets =
        offsets(container.offsets, batchOffset + numRows) + batchOffset;
    const auto firstElement = elements.batchRows();
    uint64_t numElements = 0;
    if (lengths->isFlatEncoding()) {
      const auto* sizes =
          lengths->asUnchecked<FlatVector<int32_t>>()->rawValues() +
          payloadOffset;
      for (uint32_t i = 0; i < numRows; ++i) {
        entryOffsets[i] =
            static_cast<vector_size_t>(firstElement + numElements);
        numElements += static_cast<uint32_t>(sizes[i]);
      }
    } else {
      std::fill_n(entryOffsets, numRows, firstElement);
    }
    BOLT_CHECK_LE(
        numElements,
        elements.payloadRowsLeft(),
        "malformed cell shuffle payload: container lengths past their elements");
    bytes += appendGroup(elementGroup, static_cast<uint32_t>(numElements));
  }
  return bytes;
}

vector_size_t* CellNestedAssembler::offsets(
    BufferPtr& buffer,
    uint64_t entries) {
  const uint64_t size = std::max<uint64_t>(entries, 1);
  const uint64_t bytes = size * sizeof(vector_size_t);
  if (buffer == nullptr) {
    buffer = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  } else if (buffer->capacity() < bytes) {
    const uint64_t grown = std::min<uint64_t>(
        std::max<uint64_t>(size, 2 * buffer->size() / sizeof(vector_size_t)),
        CellNestedLayout::kMaxPositions);
    AlignedBuffer::reallocate<vector_size_t>(&buffer, grown);
  }
  // size() must cover every entry: a vector validates its offsets against
  // it, and resizing keeps no more of them.
  if (buffer->size() < bytes) {
    buffer->setSize(bytes);
  }
  return buffer->asMutable<vector_size_t>();
}

void CellNestedAssembler::ContainerColumn::appendLengths(
    const VectorPtr& payloadLengths,
    vector_size_t from,
    vector_size_t at,
    vector_size_t count) {
  if (count == 0) {
    return;
  }
  if (at == 0) {
    firstPayload = payloadLengths;
    firstOffset = from;
    return;
  }
  if (firstPayload == payloadLengths) {
    BOLT_DCHECK_EQ(firstOffset + at, from);
    return;
  }
  const auto rows = at + count;
  auto* pool = payloadLengths->pool();
  auto* sizes = reinterpret_cast<vector_size_t*>(
      resizeBuffer(mergedSizes, rows * sizeof(vector_size_t), pool));
  uint64_t* nulls = nullptr;
  if (mergedNulls || (firstPayload && firstPayload->mayHaveNulls()) ||
      payloadLengths->mayHaveNulls()) {
    const bool firstNulls = !mergedNulls;
    nulls = reinterpret_cast<uint64_t*>(
        resizeBuffer(mergedNulls, bits::nwords(rows) * sizeof(uint64_t), pool));
    if (firstNulls && !firstPayload) {
      bits::fillBits(nulls, 0, at, bits::kNotNull);
    }
  }
  const auto copyRange = [&](const VectorPtr& source,
                             vector_size_t dest,
                             vector_size_t begin,
                             vector_size_t size) {
    if (source->isFlatEncoding()) {
      const auto* values =
          source->asUnchecked<FlatVector<int32_t>>()->rawValues();
      std::memcpy(sizes + dest, values + begin, size * sizeof(int32_t));
    } else {
      std::fill_n(sizes + dest, size, 0);
    }
    if (nulls) {
      if (source->isFlatEncoding() && source->rawNulls()) {
        bits::copyBits(source->rawNulls(), begin, nulls, dest, size);
      } else {
        bits::fillBits(
            nulls,
            dest,
            dest + size,
            source->isFlatEncoding() ? bits::kNotNull : bits::kNull);
      }
    }
  };
  if (firstPayload) {
    copyRange(firstPayload, 0, firstOffset, at);
    firstPayload.reset();
  }
  copyRange(payloadLengths, at, from, count);
}

std::pair<BufferPtr, BufferPtr>
CellNestedAssembler::ContainerColumn::finishLengths(
    vector_size_t count,
    memory::MemoryPool* pool) {
  if (mergedSizes) {
    return {std::move(mergedSizes), std::move(mergedNulls)};
  }
  auto source = std::move(firstPayload);
  if (source && source->isFlatEncoding()) {
    const auto& sizes = source->asUnchecked<FlatVector<int32_t>>()->values();
    if (count == source->size()) {
      return {sizes, source->nulls()};
    }
    return {
        Buffer::slice<int32_t>(sizes, firstOffset, count, pool),
        source->nulls()
            ? Buffer::slice<bool>(source->nulls(), firstOffset, count, pool)
            : nullptr};
  }
  // Only the output batch needs materialized sizes for an all-null payload.
  return {
      AlignedBuffer::allocate<int32_t>(count, pool, 0),
      count == 0 ? nullptr : allocateNulls(count, pool, bits::kNull)};
}

RowVectorPtr CellNestedAssembler::assemble() {
  std::vector<RowVectorPtr> batches;
  batches.reserve(layout_.numGroups());
  for (const auto& decoder : decoders_) {
    batches.push_back(decoder->finishBatch());
    BOLT_CHECK_NOT_NULL(batches.back(), "a column group's batch never opened");
  }
  // Children have larger ids than their parent (pre-order), so going down
  // from the last node builds them first.
  std::vector<VectorPtr> vectors(layout_.numNodes());
  for (uint32_t n = layout_.numNodes(); n-- > 0;) {
    const auto& node = layout_.node(n);
    if (node.kind == Kind::kLeaf && node.type->kind() == TypeKind::UNKNOWN) {
      vectors[n] = BaseVector::createNullConstant(
          node.type, batches[node.group]->size(), pool_);
      continue;
    }
    if (node.kind == Kind::kLeaf) {
      vectors[n] = std::move(batches[node.group]->childAt(node.valueColumn));
      continue;
    }
    if (node.kind == Kind::kRow) {
      const auto& column = batches[node.group]->childAt(node.valueColumn);
      // The structs are null where the presence is; its values are not
      // read. The fields have its positions, nulls included.
      std::vector<VectorPtr> fields;
      fields.reserve(node.children.size());
      for (const uint32_t child : node.children) {
        fields.push_back(std::move(vectors[child]));
      }
      vectors[n] = std::make_shared<RowVector>(
          pool_, node.type, column->nulls(), column->size(), std::move(fields));
      continue;
    }
    auto& container = containers_[layout_.node(node.children[0]).group];
    const auto count = batches[node.group]->size();
    auto [sizes, nulls] = container.finishLengths(count, pool_);
    if (node.kind == Kind::kArray) {
      vectors[n] = std::make_shared<ArrayVector>(
          pool_,
          node.type,
          std::move(nulls),
          count,
          std::move(container.offsets),
          std::move(sizes),
          std::move(vectors[node.children[0]]));
    } else {
      vectors[n] = std::make_shared<MapVector>(
          pool_,
          node.type,
          std::move(nulls),
          count,
          std::move(container.offsets),
          std::move(sizes),
          std::move(vectors[node.children[0]]),
          std::move(vectors[node.children[1]]));
    }
  }
  std::vector<VectorPtr> children;
  children.reserve(layout_.topNodes().size());
  for (const uint32_t n : layout_.topNodes()) {
    children.push_back(std::move(vectors[n]));
  }
  return std::make_shared<RowVector>(
      pool_,
      layout_.logicalType(),
      nullptr,
      batches[0]->size(),
      std::move(children));
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
