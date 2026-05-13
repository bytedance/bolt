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
#include "bolt/row/CompactRowLazyCodec.h"

#include <mutex>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/row/CompactRow.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::row {
namespace {

RowVectorPtr wrapAsRow(const VectorPtr& input, memory::MemoryPool* pool) {
  // Do not propagate input->nulls() onto the wrapper — its capacity may be
  // smaller than bits::nbytes(input->size()) when the source was wrapped or
  // peeled upstream, which trips the BaseVector capacity check. The wrapper
  // only exists to feed CompactRow; the encode loop reads nulls directly off
  // input via input->rawNulls(), and CompactRow itself decodes through to
  // the child so the outer ROW's nulls don't matter.
  return std::make_shared<RowVector>(
      pool,
      ROW({input->type()}),
      /*nulls=*/nullptr,
      input->size(),
      std::vector<VectorPtr>{input});
}

} // namespace

std::shared_ptr<LazyComplexVector> CompactRowLazyCodec::encode(
    const VectorPtr& input,
    memory::MemoryPool* pool) const {
  const auto rowVec = wrapAsRow(input, pool);
  CompactRow compact(rowVec);

  const auto size = input->size();
  const auto* rawNulls = input->rawNulls();

  // Size pass: null rows contribute 0 bytes (invariant: the encoded
  // StringView at a null row has size() == 0; decode() synthesizes the
  // 1-byte null payload when needed). Null rows can therefore be skipped
  // unconditionally in downstream wire-packing loops.
  // encodeToLazy enforces a complex (Row/Array/Map) input, so the wrapper
  // ROW({complexType}) is always variable-width — the fixed-size fast
  // path doesn't apply here.
  std::vector<int32_t> offsets(size + 1, 0);
  int64_t total = 0;
  for (vector_size_t i = 0; i < size; ++i) {
    offsets[i] = static_cast<int32_t>(total);
    const bool isNull = rawNulls != nullptr && bits::isBitNull(rawNulls, i);
    if (!isNull) {
      const auto rs = compact.rowSize(i);
      BOLT_CHECK_LT(
          static_cast<int64_t>(rs),
          static_cast<int64_t>(1) << 32,
          "complex-type row exceeds 4GB serialized size");
      total += rs;
    }
  }
  offsets[size] = static_cast<int32_t>(total);

  auto arena = AlignedBuffer::allocate<char>(total > 0 ? total : 1, pool, '\0');
  auto* base = arena->asMutable<char>();
  for (vector_size_t i = 0; i < size; ++i) {
    const bool isNull = rawNulls != nullptr && bits::isBitNull(rawNulls, i);
    if (!isNull) {
      compact.serialize(i, base + offsets[i]);
    }
  }

  auto valuesBuf = AlignedBuffer::allocate<StringView>(size, pool);
  auto* rawViews = valuesBuf->asMutable<StringView>();
  for (vector_size_t i = 0; i < size; ++i) {
    const auto len = offsets[i + 1] - offsets[i];
    rawViews[i] = len > 0 ? StringView(base + offsets[i], len) : StringView();
  }
  // Cannot reuse input->nulls() directly: its capacity may be smaller than
  // bits::nbytes(size) when the source vector was wrapped/sliced/peeled, and
  // the BaseVector constructor BOLT_CHECKs nulls->capacity() >= byteSize(len).
  // Copy into a freshly sized buffer when nulls are actually present.
  BufferPtr nullsBuf;
  if (rawNulls != nullptr) {
    nullsBuf = AlignedBuffer::allocate<bool>(size, pool, bits::kNotNull);
    std::memcpy(nullsBuf->asMutable<char>(), rawNulls, bits::nbytes(size));
  }
  auto flat = std::make_shared<FlatVector<StringView>>(
      pool,
      VARBINARY(),
      std::move(nullsBuf),
      size,
      valuesBuf,
      std::vector<BufferPtr>{arena});

  return std::make_shared<LazyComplexVector>(pool, input->type(), flat);
}

VectorPtr CompactRowLazyCodec::decode(
    const LazyComplexVector& lazy,
    const SelectivityVector& rows,
    memory::MemoryPool* pool) const {
  BOLT_CHECK_LE(rows.end(), lazy.size());
  const auto rowType = ROW({lazy.type()});
  std::vector<std::string_view> views;
  views.reserve(rows.end());
  // Access rawValues directly (not valueAt which returns a copy) so that
  // inlined StringViews (size <= 12 bytes) resolve data() to stable memory
  // inside the FlatVector buffer rather than to a temporary's prefix_ field.
  const auto* rawSVs = lazy.encoded()->rawValues<StringView>();
  const auto* flatBytes = lazy.encoded().get();

  // Serialized encoding of a null single-field wrapper row: the null-flags
  // byte has bit 0 set (field 0 is null), no field data follows.  This is a
  // valid CompactRow payload that deserializeRows can safely read for rows
  // whose outer LazyComplexVector null bit is set.  After spilling and
  // restoring, extractValuesWithNulls<StringView> leaves the StringView VALUE
  // uninitialized for null rows (only the null bit is set), so we must not
  // pass those garbage pointers to CompactRow::deserialize.
  static constexpr char kNullRowBytes = '\x01';

  for (vector_size_t i = 0; i < rows.end(); ++i) {
    if (flatBytes->isNullAt(i)) {
      views.emplace_back(&kNullRowBytes, 1);
    } else {
      views.emplace_back(rawSVs[i].data(), rawSVs[i].size());
    }
  }
  auto deserialized = CompactRow::deserialize(views, rowType, pool);
  return deserialized->childAt(0);
}

void ensureCompactRowLazyCodecRegistered() {
  static std::once_flag kOnce;
  std::call_once(kOnce, []() {
    bytedance::bolt::LazyComplexCodec::registerCodec(
        std::make_unique<CompactRowLazyCodec>());
  });
}

} // namespace bytedance::bolt::row
