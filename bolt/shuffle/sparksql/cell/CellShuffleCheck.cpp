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

#include "bolt/shuffle/sparksql/cell/CellShuffleCheck.h"
#include "bolt/shuffle/sparksql/Options.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <type_traits>

#include "bolt/shuffle/sparksql/cell/CellSplitter.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

template <typename T>
using ValueBits = std::conditional_t<
    sizeof(T) == 8,
    uint64_t,
    std::conditional_t<
        sizeof(T) == 4,
        uint32_t,
        std::conditional_t<sizeof(T) == 2, uint16_t, uint8_t>>>;

template <typename T>
FOLLY_ALWAYS_INLINE ValueBits<T> canonicalBits(T value) {
  using UInt = ValueBits<T>;
  const auto bits = std::bit_cast<UInt>(value);
  if constexpr (std::is_floating_point_v<T>) {
    constexpr UInt sign = UInt{1} << (sizeof(T) * 8 - 1);
    constexpr UInt exponent = sizeof(T) == 8
        ? static_cast<UInt>(0x7ff0000000000000ULL)
        : UInt{0x7f800000U};
    constexpr UInt fraction = sizeof(T) == 8
        ? static_cast<UInt>(0x000fffffffffffffULL)
        : UInt{0x007fffffU};
    constexpr UInt nan = sizeof(T) == 8
        ? static_cast<UInt>(0x7ff8000000000000ULL)
        : UInt{0x7fc00000U};
    const auto magnitude = bits & ~sign;
    if (magnitude == 0) {
      return 0; // +0 and -0 compare equal.
    }
    if ((magnitude & exponent) == exponent && (magnitude & fraction) != 0) {
      return nan; // All NaN signs/payloads compare equal.
    }
  }
  return bits;
}

template <typename T>
FOLLY_ALWAYS_INLINE uint32_t crc32cValue(uint32_t crc, T value) {
  return simd::crc32U64(crc, static_cast<uint64_t>(canonicalBits(value)));
}

template <typename T>
FOLLY_ALWAYS_INLINE uint32_t
crc32cValues(uint32_t crc, const char* data, uint32_t bytes) {
  for (uint32_t at = 0; at < bytes; at += sizeof(T)) {
    crc = crc32cValue(crc, memory::loadUnaligned<T>(data + at));
  }
  return crc;
}

memory::MemoryPool& checkedPool(memory::MemoryPool* pool) {
  BOLT_CHECK_NOT_NULL(pool);
  return *pool;
}

} // namespace

std::unique_ptr<CellShuffleCheck> CellShuffleCheck::create(
    const RowType& type,
    uint32_t numPartitions,
    const ShuffleWriterOptions& options,
    bool supportedSchema,
    memory::MemoryPool* pool) {
  if (!(options.shuffleCheckRatio > 0) || options.shuffleCheckMaxColumns <= 0) {
    return nullptr;
  }
  if (!supportedSchema) {
    LOG(INFO) << "Cell shuffle check skips nested/packed schemas";
    return nullptr;
  }
  return create(
      type,
      numPartitions,
      options.shuffleCheckRatio,
      options.shuffleCheckMaxColumns,
      pool,
      options.taskAttemptId);
}

std::unique_ptr<CellShuffleCheck> CellShuffleCheck::create(
    const RowType& type,
    uint32_t numPartitions,
    double ratio,
    int32_t maxColumns,
    memory::MemoryPool* pool,
    int64_t taskAttemptId) {
  BOLT_CHECK_GT(numPartitions, 0);
  BOLT_CHECK_LT(numPartitions, std::numeric_limits<int32_t>::max());
  checkedPool(pool);
  ratio = std::isnan(ratio) ? 0.0 : std::clamp(ratio, 0.0, 1.0);
  if (ratio == 0.0 || maxColumns <= 0) {
    return nullptr;
  }
  if (taskAttemptId % 100 >= 100 * ratio) {
    return nullptr;
  }
  std::vector<ColumnCheck> columns(type.size());
  uint32_t selectedColumns = 0;
  for (uint32_t col = 0;
       col < type.size() && selectedColumns < static_cast<uint32_t>(maxColumns);
       ++col) {
    const auto& child = type.childAt(col);
    // Short decimals share BIGINT storage but are outside the V1/V2 check.
    if (child->isDecimal()) {
      continue;
    }
    ObserveInput observeInput = nullptr;
    StreamObserve streamObserve = nullptr;
    switch (child->kind()) {
      case TypeKind::TINYINT:
        observeInput = &observeInputColumn<int8_t>;
        streamObserve = &observeStream<int8_t>;
        break;
      case TypeKind::SMALLINT:
        observeInput = &observeInputColumn<int16_t>;
        streamObserve = &observeStream<int16_t>;
        break;
      case TypeKind::INTEGER:
        observeInput = &observeInputColumn<int32_t>;
        streamObserve = &observeStream<int32_t>;
        break;
      case TypeKind::BIGINT:
        observeInput = &observeInputColumn<int64_t>;
        streamObserve = &observeStream<int64_t>;
        break;
      case TypeKind::REAL:
        observeInput = &observeInputColumn<float>;
        streamObserve = &observeStream<float>;
        break;
      case TypeKind::DOUBLE:
        observeInput = &observeInputColumn<double>;
        streamObserve = &observeStream<double>;
        break;
      default:
        continue;
    }
    columns[col].observeInput = observeInput;
    columns[col].streamObserve = streamObserve;
    columns[col].streamIndex = selectedColumns++;
  }
  if (selectedColumns == 0) {
    return nullptr;
  }
  return std::unique_ptr<CellShuffleCheck>(new CellShuffleCheck(
      numPartitions, std::move(columns), selectedColumns, pool));
}

CellShuffleCheck::CellShuffleCheck(
    uint32_t numPartitions,
    std::vector<ColumnCheck> columns,
    uint32_t selectedColumns,
    memory::MemoryPool* pool)
    : numPartitions_(numPartitions),
      columns_(std::move(columns)),
      streamStates_(memory::StlAllocator<StreamState>(checkedPool(pool))) {
  const uint64_t size = uint64_t{numPartitions_} * selectedColumns;
  BOLT_CHECK_LE(size, std::numeric_limits<int32_t>::max());
  streamStates_.resize(static_cast<int32_t>(size));
}

bool CellShuffleCheck::checksColumn(uint32_t col) const {
  return col < columns_.size() && columns_[col].observeInput != nullptr;
}

void CellShuffleCheck::setColumnCache(
    uint32_t col,
    const char* streamCacheBase) {
  BOLT_CHECK(
      checksColumn(col), "Cell shuffle check unsupported column {}", col);
  BOLT_CHECK_NOT_NULL(streamCacheBase);
  BOLT_CHECK_NULL(
      columns_[col].streamCache,
      "Cell shuffle check cache already set for column {}",
      col);
  columns_[col].streamCache = streamCacheBase;
}

void CellShuffleCheck::observeBatch(const SplitBatch& batch) {
  if (batch.numRows() == 0) {
    return;
  }
  BOLT_CHECK_LE(
      batch.row2Partition.size(), std::numeric_limits<int32_t>::max());
  BOLT_CHECK_EQ(batch.partition2RowCount.size(), numPartitions_);
  BOLT_CHECK_EQ(batch.decoded.size(), columns_.size());
  const uint32_t numRows = batch.numRows();
  uint64_t total = 0;
  for (const auto count : batch.partition2RowCount) {
    total += count;
  }
  BOLT_CHECK_EQ(total, numRows, "Cell shuffle check invalid partition counts");
  // Validate all pids once before any expected-digest scatter. A reduction can
  // vectorize, and the expected source pass needs no per-value bounds branch.
  uint32_t maxPid = 0;
  for (const auto pid : batch.row2Partition) {
    maxPid = std::max(maxPid, pid);
  }
  BOLT_CHECK_LT(maxPid, numPartitions_, "Cell shuffle check invalid partition");
  for (uint32_t col = 0; col < columns_.size(); ++col) {
    if (columns_[col].observeInput != nullptr) {
      columns_[col].observeInput(*this, col, batch);
    }
  }
}

template <typename T>
void CellShuffleCheck::observeInputColumn(
    CellShuffleCheck& check,
    uint32_t col,
    const SplitBatch& batch) {
  const auto& column = check.columns_[col];
  auto* states = check.streamStates_.data() +
      size_t{column.streamIndex} * check.numPartitions_;
  auto& decoded = batch.decoded[col];
  const uint32_t* pids = batch.row2Partition.data();
  const uint32_t numRows = batch.numRows();
  const auto observe = [&]<InputMapping kMapping>() {
    const T* values = decoded.data<T>();
    const auto* indices =
        kMapping == InputMapping::kDictionary ? decoded.indices() : nullptr;
    const int32_t constantIndex =
        kMapping == InputMapping::kConstant ? decoded.index(0) : 0;
    const auto appendExpected = [&](uint32_t row) {
      const auto index = kMapping == InputMapping::kDictionary ? indices[row]
          : kMapping == InputMapping::kConstant                ? constantIndex
                                                               : row;
      auto& state = states[pids[row]];
      state.expectedCrc = crc32cValue(state.expectedCrc, values[index]);
      ++state.expectedCount;
    };
    if (!decoded.mayHaveNulls()) {
      for (uint32_t row = 0; row < numRows; ++row) {
        appendExpected(row);
      }
      return;
    }
    if constexpr (kMapping == InputMapping::kIdentity) {
      const auto* base = decoded.base();
      if (base != nullptr && base->isFlatEncoding() &&
          !decoded.hasExtraNulls()) {
        // Read original source nulls, including slice bounds and dirty padding.
        if (const auto* nulls = base->rawNulls()) {
          bits::forEachSetBit(nulls, 0, numRows, appendExpected);
        } else {
          for (uint32_t row = 0; row < numRows; ++row) {
            appendExpected(row);
          }
        }
        return;
      }
    }
    // Do not load dictionary indices/values hidden by wrapping nulls.
    for (uint32_t row = 0; row < numRows; ++row) {
      if (!decoded.isNullAt(row)) {
        appendExpected(row);
      }
    }
  };
  dispatchInputMapping(decoded, observe);
}

template <typename T>
FOLLY_NOINLINE void CellShuffleCheck::observeStream(
    CellShuffleCheck& check,
    uint32_t col,
    uint32_t pid,
    const char* line,
    uint32_t usedBytes) {
  BOLT_CHECK_LT(pid, check.numPartitions_);
  const auto& column = check.columns_[col];
  BOLT_CHECK_NOT_NULL(column.streamCache);
  BOLT_CHECK(line == column.streamCache + size_t{pid} * kCacheLineBytes);
  BOLT_CHECK_LE(usedBytes, kCacheLineBytes);
  BOLT_CHECK_EQ(usedBytes % sizeof(T), 0);
  auto& state = check.streamStates_
                    [size_t{column.streamIndex} * check.numPartitions_ + pid];
  const uint32_t count = usedBytes / sizeof(T);
  state.actualCount += count;
  BOLT_CHECK_LE(
      state.actualCount,
      state.expectedCount,
      "Cell streaming check extra non-null value: column={} partition={}",
      col,
      pid);
  state.actualCrc = crc32cValues<T>(state.actualCrc, line, usedBytes);
}

void CellShuffleCheck::beforeStreamFlush(
    uint32_t col,
    uint32_t pid,
    const char* line,
    uint32_t usedBytes) {
  BOLT_CHECK(checksColumn(col));
  BOLT_CHECK_LT(pid, numPartitions_);
  const auto observe = columns_[col].streamObserve;
  BOLT_CHECK(observe != nullptr);
  observe(*this, col, pid, line, usedBytes);
}

void CellShuffleCheck::finishStream() {
  for (uint32_t col = 0; col < columns_.size(); ++col) {
    if (!checksColumn(col)) {
      continue;
    }
    const auto* states = streamStates_.data() +
        size_t{columns_[col].streamIndex} * numPartitions_;
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      const auto& state = states[pid];
      BOLT_CHECK_EQ(
          state.actualCount,
          state.expectedCount,
          "Cell streaming check count mismatch: column={} partition={}",
          col,
          pid);
      BOLT_CHECK_EQ(
          state.actualCrc,
          state.expectedCrc,
          "Cell streaming check CRC mismatch: column={} partition={}",
          col,
          pid);
    }
  }
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
