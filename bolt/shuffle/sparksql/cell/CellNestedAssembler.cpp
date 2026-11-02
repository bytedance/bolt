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

/// Lengths decoded at a time while counting a space's positions.
constexpr uint32_t kCountBatch = 1024;

} // namespace

CellNestedAssembler::CellNestedAssembler(
    RowTypePtr type,
    CellDecompressor* decompressor,
    memory::MemoryPool* pool,
    CellDecodeLimits limits,
    uint32_t appendPositions)
    : layout_(std::move(type)),
      pool_(pool),
      limits_(limits),
      appendPositions_(appendPositions),
      reader_(layout_.layout(), decompressor, pool, limits),
      positions_(layout_.numSpaces(), 0),
      offsets_(layout_.numNodes()) {
  BOLT_CHECK_GT(appendPositions_, 0);
  // The spaces' decoders open what reader_ decompressed.
  for (uint32_t s = 0; s < layout_.numSpaces(); ++s) {
    decoders_.push_back(std::make_unique<CellPayloadDecoder>(
        CellLayout::create(layout_.space(s).rowType), nullptr, pool, limits));
  }
}

bool CellNestedAssembler::beginPayload(CellByteSource& in, std::string& error) {
  uint32_t rowCount = 0;
  if (!reader_.readPayload(in, rowCount, error)) {
    return false;
  }
  positions_[0] = rowCount;
  nullBytes_ = 0;
  // Spaces are numbered after their owners': a space's positions are known
  // by the time it opens.
  for (uint32_t s = 0; s < layout_.numSpaces(); ++s) {
    const auto& space = layout_.space(s);
    if (limits_.maxDecodedBytes != 0) {
      // Each decoder bounds its own bitmaps; together, every column of
      // every space at most.
      nullBytes_ += space.splitColumns.size() * ((positions_[s] + 7) / 8 + 8);
      if (nullBytes_ > limits_.maxDecodedBytes) {
        return fail(error, "null bitmaps beyond sanity bound");
      }
    }
    if (!decoders_[s]->openPayload(
            reader_,
            space.columns.data(),
            static_cast<uint32_t>(positions_[s]),
            error)) {
      return false;
    }
    for (const uint32_t x : space.containers) {
      if (!countPositions(x, error)) {
        return false;
      }
    }
  }
  // Every space's fixed bytes and string data, per row.
  uint64_t bytes = 0;
  stringsLeft_ = 0;
  for (uint32_t s = 0; s < layout_.numSpaces(); ++s) {
    const auto& decoder = *decoders_[s];
    bytes += positions_[s] * decoder.rowBytes();
    const auto& cellLayout = decoder.layout();
    for (uint32_t col = 0; col < cellLayout.numColumns(); ++col) {
      if (cellLayout.isStringColumn(col)) {
        stringsLeft_ +=
            decoder.streamBytes(cellLayout.columnStream(col) + 1).size();
      }
    }
  }
  rowBytes_ = std::max<uint64_t>(
      1, (bytes + stringsLeft_) / std::max<uint32_t>(1, rowCount));
  return true;
}

bool CellNestedAssembler::countPositions(uint32_t x, std::string& error) {
  const auto& node = layout_.node(x);
  const auto& decoder = *decoders_[node.space];
  const uint32_t col = node.spaceColumn;
  const auto bytes = decoder.streamBytes(decoder.layout().columnStream(col));
  StreamCursor<int32_t> cursor;
  cursor.reset(
      reinterpret_cast<const uint8_t*>(bytes.data()),
      bytes.size(),
      decoder.nonNullCount(col));
  // The stream is decoded once more when its rows are appended; the
  // lengths are not kept.
  uint64_t total = 0;
  int32_t lengths[kCountBatch];
  for (uint32_t left = decoder.nonNullCount(col); left > 0;) {
    const uint32_t n = std::min(left, kCountBatch);
    if (!cursor.next(n, lengths)) {
      return fail(error, "malformed container length stream");
    }
    for (uint32_t i = 0; i < n; ++i) {
      if (lengths[i] < 0) {
        return fail(error, "negative container length");
      }
      total += static_cast<uint64_t>(lengths[i]);
    }
    if (total > CellNestedLayout::kMaxPositions) {
      return fail(error, "container lengths beyond the position limit");
    }
    left -= n;
  }
  if (!cursor.finished()) {
    return fail(error, "malformed container length stream");
  }
  positions_[node.ownedSpace] = total;
  return true;
}

bool CellNestedAssembler::fits() const {
  for (uint32_t s = 1; s < layout_.numSpaces(); ++s) {
    if (static_cast<uint64_t>(decoders_[s]->batchRows()) + positions_[s] >
        CellNestedLayout::kMaxPositions) {
      return false;
    }
  }
  return limits_.maxDecodedBytes == 0 || restBytes() <= limits_.maxDecodedBytes;
}

uint64_t CellNestedAssembler::batchBytes() const {
  uint64_t bytes = 0;
  for (const auto& decoder : decoders_) {
    bytes += decoder->batchBytes();
  }
  return bytes;
}

uint64_t CellNestedAssembler::positionBytes(uint32_t s, uint64_t positions)
    const {
  const auto& space = layout_.space(s);
  return positions *
      (decoders_[s]->rowBytes() +
       sizeof(vector_size_t) * space.containers.size()) +
      (positions * space.splitColumns.size() + 7) / 8;
}

uint64_t CellNestedAssembler::batchStrings() const {
  uint64_t bytes = 0;
  for (const auto& decoder : decoders_) {
    // The bytes its batch's strings take out of line.
    bytes += decoder->batchBytes() -
        static_cast<uint64_t>(decoder->batchRows()) * decoder->rowBytes();
  }
  return bytes;
}

uint64_t CellNestedAssembler::heldBytes() const {
  uint64_t bytes = nullBytes_ + batchStrings();
  for (uint32_t s = 0; s < layout_.numSpaces(); ++s) {
    bytes += 2 * positionBytes(s, decoders_[s]->batchRows());
  }
  return bytes;
}

uint64_t CellNestedAssembler::restBytes() const {
  uint64_t bytes = nullBytes_ + batchStrings() + stringsLeft_;
  for (uint32_t s = 0; s < layout_.numSpaces(); ++s) {
    const auto& decoder = *decoders_[s];
    bytes += 2 *
        positionBytes(
                 s,
                 static_cast<uint64_t>(decoder.batchRows()) +
                     decoder.payloadRowsLeft());
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
      if (batchRows > 0 && !fits()) {
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
          n, std::max<uint64_t>(1, (budget - used) / rowBytes_));
    }
    const uint64_t bound = limits_.maxDecodedBytes;
    if (bound != 0) {
      const uint64_t rest = restBytes();
      if (rest > bound) {
        // The rows the bound has room for, by the bytes per row of the
        // payload's rows left; a row that alone passes it fails below.
        const uint64_t held = heldBytes();
        const uint64_t left = rows.payloadRowsLeft();
        const uint64_t perRow = (rest - held + left - 1) / left;
        n = std::min(n, (bound - std::min(held, bound)) / perRow);
        if (n == 0) {
          if (batchRows > 0) {
            break;
          }
          n = 1;
        }
      }
    }
    const uint64_t strings = batchStrings();
    appendSpace(0, n, capacity);
    // Out of line, the strings just read take at most their stream bytes,
    // unless a dictionary repeats them.
    stringsLeft_ -= std::min(stringsLeft_, batchStrings() - strings);
  }
  return rows.batchRows() == 0 ? nullptr : assemble();
}

void CellNestedAssembler::appendSpace(
    uint32_t s,
    uint64_t positions,
    vector_size_t capacity) {
  auto& decoder = *decoders_[s];
  const auto& space = layout_.space(s);
  const vector_size_t at = decoder.batchRows();
  if (limits_.maxDecodedBytes != 0) {
    // Checked before any of the positions is read; the strings they hold
    // count once they are.
    BOLT_CHECK_LE(
        heldBytes() +
            2 * (positionBytes(s, at + positions) - positionBytes(s, at)),
        limits_.maxDecodedBytes,
        "malformed cell shuffle payload: batch beyond sanity bound");
  }
  // A batch of the rows opens at the reader's capacity, one of elements at
  // its first append's: it grows from there as entries need it.
  const vector_size_t open = s == 0
      ? capacity
      : static_cast<vector_size_t>(std::max<uint64_t>(positions, 1));
  std::string error;
  uint64_t left = positions;
  do {
    const auto piece =
        static_cast<uint32_t>(std::min<uint64_t>(left, appendPositions_));
    const bool ok = decoder.appendRows(piece, open, error);
    BOLT_CHECK(ok, "malformed cell shuffle payload: {}", error);
    left -= piece;
  } while (left > 0);

  for (const uint32_t x : space.containers) {
    const auto& node = layout_.node(x);
    auto& elements = *decoders_[node.ownedSpace];
    // A null entry's length reads 0.
    const auto* lengths =
        decoder.batchValues(node.spaceColumn)->as<int32_t>() + at;
    vector_size_t* entryOffsets = offsets(x, at + positions);
    const uint64_t base = elements.batchRows();
    uint64_t total = 0;
    for (uint64_t i = 0; i < positions; ++i) {
      // Within one vector: fits() held when the payload began.
      entryOffsets[at + i] = static_cast<vector_size_t>(base + total);
      total += static_cast<uint64_t>(lengths[i]);
    }
    BOLT_CHECK_LE(
        total,
        elements.payloadRowsLeft(),
        "malformed cell shuffle payload: container lengths past their elements");
    appendSpace(node.ownedSpace, total, capacity);
  }
}

vector_size_t* CellNestedAssembler::offsets(uint32_t x, uint64_t entries) {
  auto& buffer = offsets_[x];
  const uint64_t size = std::max<uint64_t>(entries, 1);
  const uint64_t bytes = size * sizeof(vector_size_t);
  if (buffer == nullptr) {
    buffer = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  } else if (buffer->capacity() < bytes) {
    AlignedBuffer::reallocate<vector_size_t>(
        &buffer,
        std::max<uint64_t>(
            entries, 2 * buffer->size() / sizeof(vector_size_t)));
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
  batches.reserve(layout_.numSpaces());
  for (const auto& decoder : decoders_) {
    batches.push_back(decoder->finishBatch());
    BOLT_CHECK_NOT_NULL(batches.back(), "a space's batch never opened");
  }
  // Bottom up: a node's children are built when it is.
  std::vector<VectorPtr> vectors(layout_.numNodes());
  for (uint32_t n = layout_.numNodes(); n-- > 0;) {
    const auto& node = layout_.node(n);
    auto& column = batches[node.space]->childAt(node.spaceColumn);
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

std::unique_ptr<CellBatchSource> makeCellNestedAssembler(
    const RowTypePtr& type,
    CellDecompressor* decompressor,
    memory::MemoryPool* pool,
    CellDecodeLimits limits) {
  if (!CellNestedLayout::supports(type)) {
    return nullptr;
  }
  return std::make_unique<CellNestedAssembler>(
      type, decompressor, pool, limits);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
