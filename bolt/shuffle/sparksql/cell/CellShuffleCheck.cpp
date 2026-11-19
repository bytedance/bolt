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
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <type_traits>

#include "bolt/shuffle/sparksql/cell/CellSplitter.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

#if defined(__SSE4_2__)
#include <nmmintrin.h>
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

// Reflected Castagnoli polynomial. The portable path consumes the same little-
// endian byte stream as the x86/ARM CRC32C instructions, without final
// inversion.
constexpr auto makeCrc32cTable() {
  std::array<uint32_t, 256> table{};
  for (uint32_t byte = 0; byte < table.size(); ++byte) {
    auto crc = byte;
    for (uint32_t bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1) ^ ((crc & 1) ? 0x82f63b78U : 0);
    }
    table[byte] = crc;
  }
  return table;
}

inline constexpr auto kCrc32cTable = makeCrc32cTable();

template <typename UInt>
FOLLY_ALWAYS_INLINE uint64_t crc32cWord(uint64_t crc, UInt value) {
  static_assert(std::is_unsigned_v<UInt>);
#if defined(__SSE4_2__)
  if constexpr (sizeof(UInt) == 8) {
#if defined(__x86_64__)
    return _mm_crc32_u64(crc, value);
#else
    crc = _mm_crc32_u32(crc, static_cast<uint32_t>(value));
    return _mm_crc32_u32(crc, static_cast<uint32_t>(value >> 32));
#endif
  } else if constexpr (sizeof(UInt) == 4) {
    return _mm_crc32_u32(crc, value);
  } else if constexpr (sizeof(UInt) == 2) {
    return _mm_crc32_u16(crc, value);
  } else {
    return _mm_crc32_u8(crc, value);
  }
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
  if constexpr (sizeof(UInt) == 8) {
    return __crc32cd(crc, value);
  } else if constexpr (sizeof(UInt) == 4) {
    return __crc32cw(crc, value);
  } else if constexpr (sizeof(UInt) == 2) {
    return __crc32ch(crc, value);
  } else {
    return __crc32cb(crc, value);
  }
#else
  for (uint32_t byte = 0; byte < sizeof(UInt); ++byte) {
    crc =
        (static_cast<uint32_t>(crc) >> 8) ^ kCrc32cTable[(crc ^ value) & 0xff];
    value >>= 8;
  }
  return crc;
#endif
}

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
FOLLY_ALWAYS_INLINE uint32_t
crc32cBytes(uint32_t crc, const char* data, uint32_t bytes) {
  uint64_t wideCrc = crc;
  while (bytes >= sizeof(uint64_t)) {
    uint64_t word;
    std::memcpy(&word, data, sizeof(word));
    wideCrc = crc32cWord(wideCrc, word);
    data += sizeof(word);
    bytes -= sizeof(word);
  }
  crc = static_cast<uint32_t>(wideCrc);
  // The caller validates that the range is aligned to T. Do not retain
  // impossible 4/2/1-byte tail branches in the BIGINT observer, for example.
  const auto tail = [&]<typename UInt>() {
    if constexpr (sizeof(T) <= sizeof(UInt)) {
      if (bytes >= sizeof(UInt)) {
        UInt word;
        std::memcpy(&word, data, sizeof(word));
        crc = crc32cWord(crc, word);
        data += sizeof(word);
        bytes -= sizeof(word);
      }
    }
  };
  tail.template operator()<uint32_t>();
  tail.template operator()<uint16_t>();
  tail.template operator()<uint8_t>();
  return crc;
}

template <typename T>
FOLLY_ALWAYS_INLINE uint32_t
crc32cValues(uint32_t crc, const char* data, uint32_t bytes) {
  if constexpr (
      std::is_integral_v<T> && std::endian::native == std::endian::little) {
    // The native-width expected updates and these bulk byte updates produce
    // the same CRC stream, including a partial line's unaligned prefix.
    return crc32cBytes<T>(crc, data, bytes);
  } else {
    uint64_t wideCrc = crc;
    for (uint32_t at = 0; at < bytes; at += sizeof(T)) {
      T actual;
      std::memcpy(&actual, data + at, sizeof(T));
      wideCrc = crc32cWord(wideCrc, canonicalBits(actual));
    }
    return static_cast<uint32_t>(wideCrc);
  }
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
  auto check = std::make_unique<CellShuffleCheck>(
      type,
      numPartitions,
      options.shuffleCheckRatio,
      options.shuffleCheckMaxColumns,
      pool,
      options.taskAttemptId);
  if (!check->enabled()) {
    return nullptr;
  }
  return check;
}

CellShuffleCheck::CellShuffleCheck(
    const RowType& type,
    uint32_t numPartitions,
    double ratio,
    int32_t maxColumns,
    memory::MemoryPool* pool,
    int64_t taskAttemptId)
    : numPartitions_(numPartitions),
      columns_(type.size()),
      streamStates_(memory::StlAllocator<StreamState>(checkedPool(pool))) {
  BOLT_CHECK_GT(numPartitions_, 0);
  BOLT_CHECK_LT(numPartitions_, std::numeric_limits<int32_t>::max());
  ratio = std::isnan(ratio) ? 0.0 : std::clamp(ratio, 0.0, 1.0);
  if (ratio == 0.0 || maxColumns <= 0) {
    return;
  }
  if (taskAttemptId % 100 >= 100 * ratio) {
    return;
  }
  for (uint32_t col = 0; col < type.size() &&
       selectedColumns_ < static_cast<uint32_t>(maxColumns);
       ++col) {
    const auto& child = type.childAt(col);
    // Short decimals share BIGINT storage but are outside the V1/V2 check.
    if (child->isDecimal()) {
      continue;
    }
    Select select;
    switch (child->kind()) {
      case TypeKind::TINYINT:
        select = &prepareColumn<int8_t>;
        break;
      case TypeKind::SMALLINT:
        select = &prepareColumn<int16_t>;
        break;
      case TypeKind::INTEGER:
        select = &prepareColumn<int32_t>;
        break;
      case TypeKind::BIGINT:
        select = &prepareColumn<int64_t>;
        break;
      case TypeKind::REAL:
        select = &prepareColumn<float>;
        break;
      case TypeKind::DOUBLE:
        select = &prepareColumn<double>;
        break;
      default:
        continue;
    }
    columns_[col].select = select;
    columns_[col].streamIndex = selectedColumns_++;
  }
}

void CellShuffleCheck::prepareInput(const RowVector& input, bool isHash) {
  BOLT_CHECK(batch_ == nullptr, "Cell shuffle check batch already active");
  sourcePids_ = nullptr;
  preparedNumRows_ = 0;
  if (!enabled() || !isHash || input.size() == 0 ||
      input.childrenSize() != columns_.size() + 1) {
    return;
  }
  const auto& source = input.childAt(0);
  if (source != nullptr && source->typeKind() == TypeKind::INTEGER &&
      source->isFlatEncoding() && !source->mayHaveNulls() &&
      source->size() >= input.size()) {
    const auto* pids = source->asUnchecked<FlatVector<int32_t>>()->rawValues();
    uint32_t maxPid = 0;
    for (uint32_t row = 0; row < input.size(); ++row) {
      maxPid = std::max(maxPid, static_cast<uint32_t>(pids[row]));
    }
    if (maxPid < numPartitions_) {
      sourcePids_ = pids;
      preparedNumRows_ = input.size();
    }
  }
}

bool CellShuffleCheck::beginBatch(const SplitBatch& batch) {
  BOLT_CHECK(batch_ == nullptr, "Cell shuffle check batch already active");
  if (selectedColumns_ == 0 || batch.numRows() == 0) {
    return false;
  }
  BOLT_CHECK_LE(
      batch.row2Partition.size(), std::numeric_limits<int32_t>::max());
  BOLT_CHECK_EQ(batch.partition2RowCount.size(), numPartitions_);
  BOLT_CHECK_EQ(batch.decoded.size(), columns_.size());
  const uint32_t numRows = batch.numRows();
  if (streamStates_.empty()) {
    const uint64_t size = uint64_t{numPartitions_} * selectedColumns_;
    BOLT_CHECK_LE(size, std::numeric_limits<int32_t>::max());
    streamStates_.resize(static_cast<int32_t>(size));
  }
  if (sourcePids_ != nullptr) {
    BOLT_CHECK_EQ(preparedNumRows_, numRows);
  }
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
  // Install the active batch only after validation/allocation. endBatch also
  // clears any earlier source borrow if those steps fail.
  batch_ = &batch;
  return true;
}

bool CellShuffleCheck::checksColumn(uint32_t col) const {
  return col < columns_.size() && columns_[col].select != nullptr;
}

void CellShuffleCheck::beginColumn(uint32_t col, const char* streamCacheBase) {
  BOLT_CHECK_NOT_NULL(batch_);
  BOLT_CHECK(
      checksColumn(col), "Cell shuffle check unsupported column {}", col);
  BOLT_CHECK_NOT_NULL(streamCacheBase);
  column_ = col;
  cacheBase_ = streamCacheBase;
  columns_[col].select(*this);
}

template <typename T>
void CellShuffleCheck::prepareColumn(CellShuffleCheck& check) {
  auto& column = check.columns_[check.column_];
  column.streamCache = check.cacheBase_;
  column.streamObserve = &observeStream<T>;
  check.streamColumn_ = check.streamStates_.data() +
      size_t{column.streamIndex} * check.numPartitions_;
  auto& decoded = check.batch_->decoded[check.column_];
  const auto prepare = [&]<InputMapping kMapping>() {
    const T* values = decoded.data<T>();
    const auto* indices =
        kMapping == InputMapping::kDictionary ? decoded.indices() : nullptr;
    const int32_t constantIndex =
        kMapping == InputMapping::kConstant ? decoded.index(0) : 0;
    const uint32_t* pids = check.sourcePids_ != nullptr
        ? reinterpret_cast<const uint32_t*>(check.sourcePids_)
        : check.batch_->row2Partition.data();
    auto* states = check.streamColumn_;
    const uint32_t numRows = check.batch_->numRows();
    const auto appendExpected = [&](uint32_t row) {
      const auto index = kMapping == InputMapping::kDictionary ? indices[row]
          : kMapping == InputMapping::kConstant                ? constantIndex
                                                               : row;
      auto& state = states[pids[row]];
      state.expectedCrc =
          crc32cWord(state.expectedCrc, canonicalBits(values[index]));
      ++state.remainingCount;
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
  if (decoded.isIdentityMapping()) {
    prepare.template operator()<InputMapping::kIdentity>();
  } else if (decoded.isConstantMapping()) {
    prepare.template operator()<InputMapping::kConstant>();
  } else {
    prepare.template operator()<InputMapping::kDictionary>();
  }
  check.observe_ = &observeStream<T>;
}

void CellShuffleCheck::endBatch() noexcept {
  batch_ = nullptr;
  observe_ = nullptr;
  cacheBase_ = nullptr;
  sourcePids_ = nullptr;
  preparedNumRows_ = 0;
  streamColumn_ = nullptr;
}

template <typename T>
void CellShuffleCheck::observeStream(
    CellShuffleCheck& check,
    uint32_t col,
    uint32_t pid,
    const char* line,
    uint32_t usedBytes) {
  BOLT_CHECK_LT(pid, check.numPartitions_);
  const auto& column = check.columns_[col];
  BOLT_CHECK(line == column.streamCache + size_t{pid} * kCacheLineBytes);
  BOLT_CHECK_LE(usedBytes, kCacheLineBytes);
  BOLT_CHECK_EQ(usedBytes % sizeof(T), 0);
  auto& state = check.streamStates_
                    [size_t{column.streamIndex} * check.numPartitions_ + pid];
  const uint32_t count = usedBytes / sizeof(T);
  BOLT_CHECK_LE(
      count,
      state.remainingCount,
      "Cell streaming check extra non-null value: column={} partition={}",
      col,
      pid);
  state.actualCrc = crc32cValues<T>(state.actualCrc, line, usedBytes);
  state.remainingCount -= count;
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
  BOLT_CHECK(batch_ == nullptr);
  for (uint32_t col = 0; col < columns_.size(); ++col) {
    if (!checksColumn(col) || streamStates_.empty()) {
      continue;
    }
    const auto* states = streamStates_.data() +
        size_t{columns_[col].streamIndex} * numPartitions_;
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      const auto& state = states[pid];
      BOLT_CHECK_EQ(
          state.remainingCount,
          0,
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
