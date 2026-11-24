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

#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

using Kind = CellNestedLayout::Kind;

/// Lengths decoded at a time while counting a column group's positions.
constexpr uint32_t kLengthsPerRead = 1024;

} // namespace

CellNestedAssembler::CellNestedAssembler(
    RowTypePtr type,
    ShuffleDecompressor* decompressor,
    memory::MemoryPool* pool)
    : layout_(std::move(type)),
      pool_(pool),
      payloadDecoder_(layout_.layout(), decompressor, pool),
      payloadPositions_(layout_.numGroups(), 0),
      offsets_(layout_.numNodes()) {
  // The column groups' decoders open what payloadDecoder_ decompressed.
  for (uint32_t g = 0; g < layout_.numGroups(); ++g) {
    decoders_.push_back(std::make_unique<CellPayloadDecoder>(
        CellLayout::create(layout_.group(g).rowType), nullptr, pool));
  }
}

void CellNestedAssembler::beginPayload(ShuffleByteSource& in) {
  const uint32_t rowCount = payloadDecoder_.readPayload(in);
  payloadPositions_[0] = rowCount;
  // Column groups are numbered after their owners': a group's positions are
  // known by the time it opens.
  for (uint32_t g = 0; g < layout_.numGroups(); ++g) {
    const auto& group = layout_.group(g);
    decoders_[g]->openPayload(
        payloadDecoder_,
        group.columns,
        static_cast<uint32_t>(payloadPositions_[g]));
    for (const uint32_t x : group.containers) {
      countPositions(x);
    }
  }
  // Every column group's fixed bytes and string data, per row.
  uint64_t bytes = 0;
  uint64_t strings = 0;
  for (uint32_t g = 0; g < layout_.numGroups(); ++g) {
    const auto& decoder = *decoders_[g];
    bytes += payloadPositions_[g] * decoder.rowBytes();
    const auto& cellLayout = decoder.layout();
    for (uint32_t col = 0; col < cellLayout.numColumns(); ++col) {
      if (cellLayout.isStringColumn(col)) {
        strings += decoder.streamBytes(cellLayout.firstStream(col) + 1).size();
      }
    }
  }
  payloadRowBytes_ = std::max<uint64_t>(
      1, (bytes + strings) / std::max<uint32_t>(1, rowCount));
}

void CellNestedAssembler::countPositions(uint32_t x) {
  const auto& node = layout_.node(x);
  const auto& decoder = *decoders_[node.group];
  const uint32_t col = node.groupColumn;
  const auto bytes = decoder.streamBytes(decoder.layout().firstStream(col));
  StreamCursor<int32_t> cursor;
  cursor.reset(
      reinterpret_cast<const uint8_t*>(bytes.data()),
      bytes.size(),
      decoder.nonNullCount(col));
  // The stream is decoded once more when its rows are appended; the
  // lengths are not kept.
  uint64_t total = 0;
  int32_t lengths[kLengthsPerRead];
  for (uint32_t left = decoder.nonNullCount(col); left > 0;) {
    const uint32_t n = std::min(left, kLengthsPerRead);
    if (!cursor.next(n, lengths)) {
      failMalformedPayload("malformed container length stream");
    }
    for (uint32_t i = 0; i < n; ++i) {
      if (lengths[i] < 0) {
        failMalformedPayload("negative container length");
      }
      total += static_cast<uint64_t>(lengths[i]);
    }
    if (total > CellNestedLayout::kMaxPositions) {
      failMalformedPayload("container lengths beyond the position limit");
    }
    left -= n;
  }
  if (!cursor.finished()) {
    failMalformedPayload("malformed container length stream");
  }
  payloadPositions_[node.ownedGroup] = total;
}

bool CellNestedAssembler::takesPayload() const {
  for (uint32_t g = 1; g < layout_.numGroups(); ++g) {
    if (static_cast<uint64_t>(decoders_[g]->batchRows()) +
            payloadPositions_[g] >
        CellNestedLayout::kMaxPositions) {
      return false;
    }
  }
  return true;
}

uint64_t CellNestedAssembler::batchBytes() const {
  uint64_t bytes = 0;
  for (const auto& decoder : decoders_) {
    bytes += decoder->batchBytes();
  }
  return bytes;
}

RowVectorPtr CellNestedAssembler::nextBatch(
    vector_size_t capacity,
    int64_t byteBudget,
    const std::function<bool()>& nextPayload) {
  auto& rows = *decoders_[0];
  while (true) {
    const vector_size_t batchRows = rows.batchRows();
    if (batchRows >= capacity) {
      break;
    }
    // As for a flat payload: past the budget, or a single append when there
    // is none.
    if (batchRows > 0 &&
        (byteBudget <= 0 || static_cast<int64_t>(batchBytes()) >= byteBudget)) {
      break;
    }
    if (rows.payloadRowsLeft() == 0) {
      if (!nextPayload()) {
        // End of input, or a composite block that comes out on its own.
        break;
      }
      if (batchRows > 0 && !takesPayload()) {
        // The new payload starts the next batch, whose positions always
        // take it.
        break;
      }
      continue;
    }
    uint64_t n =
        std::min<uint64_t>(rows.payloadRowsLeft(), capacity - batchRows);
    if (byteBudget > 0) {
      // The rows the budget has room for, by the payload's bytes per row.
      const uint64_t budget = static_cast<uint64_t>(byteBudget);
      const uint64_t used = std::min(batchBytes(), budget);
      n = std::min<uint64_t>(
          n, std::max<uint64_t>(1, (budget - used) / payloadRowBytes_));
    }
    appendGroup(0, n, capacity);
  }
  return rows.batchRows() == 0 ? nullptr : assemble();
}

void CellNestedAssembler::appendGroup(
    uint32_t g,
    uint64_t positions,
    vector_size_t capacity) {
  auto& decoder = *decoders_[g];
  const auto& group = layout_.group(g);
  const vector_size_t at = decoder.batchRows();
  // A batch of the rows opens at the reader's capacity, one of elements at
  // its first append's: it grows from there as entries need it.
  const vector_size_t open = g == 0
      ? capacity
      : static_cast<vector_size_t>(std::max<uint64_t>(positions, 1));
  uint64_t left = positions;
  do {
    const auto piece =
        static_cast<uint32_t>(std::min<uint64_t>(left, kAppendPositions));
    decoder.appendRows(piece, open);
    left -= piece;
  } while (left > 0);

  for (const uint32_t x : group.containers) {
    const auto& node = layout_.node(x);
    auto& elements = *decoders_[node.ownedGroup];
    // A null entry's length reads 0.
    const auto* lengths =
        decoder.batchValues(node.groupColumn)->as<int32_t>() + at;
    vector_size_t* entryOffsets = offsets(x, at + positions);
    const uint64_t base = elements.batchRows();
    uint64_t total = 0;
    for (uint64_t i = 0; i < positions; ++i) {
      // Within one vector: takesPayload() held when the payload began.
      entryOffsets[at + i] = static_cast<vector_size_t>(base + total);
      total += static_cast<uint64_t>(lengths[i]);
    }
    BOLT_CHECK_LE(
        total,
        elements.payloadRowsLeft(),
        "malformed cell shuffle payload: container lengths past their elements");
    appendGroup(node.ownedGroup, total, capacity);
  }
}

vector_size_t* CellNestedAssembler::offsets(uint32_t x, uint64_t entries) {
  auto& buffer = offsets_[x];
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
    auto& column = batches[node.group]->childAt(node.groupColumn);
    if (node.kind == Kind::kLeaf) {
      vectors[n] = std::move(column);
      continue;
    }
    if (node.kind == Kind::kRow) {
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
    // The lengths, 0 at a null entry, are the sizes as they are.
    const auto* lengths = column->asFlatVector<int32_t>();
    BOLT_CHECK_NOT_NULL(lengths);
    BOLT_CHECK_NOT_NULL(offsets_[n], "a container's offsets never written");
    if (node.kind == Kind::kArray) {
      vectors[n] = std::make_shared<ArrayVector>(
          pool_,
          node.type,
          lengths->nulls(),
          lengths->size(),
          std::move(offsets_[n]),
          lengths->values(),
          std::move(vectors[node.children[0]]));
    } else {
      vectors[n] = std::make_shared<MapVector>(
          pool_,
          node.type,
          lengths->nulls(),
          lengths->size(),
          std::move(offsets_[n]),
          lengths->values(),
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
