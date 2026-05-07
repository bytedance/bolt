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
#include "bolt/vector/LazyComplexCodec.h"

#include <mutex>
#include <unordered_map>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/type/Type.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt {
namespace {

struct Registry {
  std::mutex mu;
  std::unordered_map<std::string, std::unique_ptr<LazyComplexCodec>> byName;
  std::string activeName;
  const LazyComplexCodec* active = nullptr;
};

Registry& registry() {
  static Registry r;
  return r;
}

} // namespace

void LazyComplexCodec::registerCodec(std::unique_ptr<LazyComplexCodec> codec) {
  auto& r = registry();
  std::lock_guard<std::mutex> g(r.mu);
  const auto name = std::string(codec->name());
  BOLT_CHECK(
      r.byName.emplace(name, std::move(codec)).second,
      "LazyComplexCodec already registered: {}",
      name);
}

void LazyComplexCodec::setActiveFormat(std::string_view name) {
  auto& r = registry();
  std::lock_guard<std::mutex> g(r.mu);
  if (name.empty()) {
    r.activeName.clear();
    r.active = nullptr;
    return;
  }
  auto it = r.byName.find(std::string(name));
  BOLT_USER_CHECK(
      it != r.byName.end(), "unknown complex_lazy_encoding format: '{}'", name);
  r.activeName = it->first;
  r.active = it->second.get();
}

const LazyComplexCodec* LazyComplexCodec::activeCodec() {
  auto& r = registry();
  std::lock_guard<std::mutex> g(r.mu);
  return r.active;
}

std::shared_ptr<LazyComplexVector> encodeToLazy(
    const VectorPtr& input,
    memory::MemoryPool* pool,
    const LazyComplexCodec& codec) {
  if (input->encoding() == VectorEncoding::Simple::LAZY_COMPLEX) {
    return std::static_pointer_cast<LazyComplexVector>(input);
  }
  BOLT_CHECK(
      input->type()->isRow() || input->type()->isArray() ||
          input->type()->isMap(),
      "encodeToLazy only supports complex types, got {}",
      input->type()->toString());
  return codec.encode(input, pool);
}

RowVectorPtr decodeLazyColumns(
    const RowVectorPtr& input,
    memory::MemoryPool* pool) {
  if (!input) {
    return input;
  }
  std::vector<VectorPtr> children = input->children();
  bool changed = false;
  SelectivityVector allRows(input->size());
  for (auto& child : children) {
    if (child && child->encoding() == VectorEncoding::Simple::LAZY_COMPLEX) {
      child = child->asUnchecked<LazyComplexVector>()->decode(allRows, pool);
      changed = true;
    }
  }
  if (!changed) {
    return input;
  }
  return std::make_shared<RowVector>(
      input->pool(),
      input->type(),
      input->nulls(),
      input->size(),
      std::move(children));
}

RowVectorPtr decodeLazyColumns(
    const RowVectorPtr& input,
    memory::MemoryPool* pool,
    const std::unordered_set<column_index_t>& columns) {
  if (!input || columns.empty()) {
    return input;
  }
  std::vector<VectorPtr> children = input->children();
  bool changed = false;
  SelectivityVector allRows(input->size());
  for (const auto colIdx : columns) {
    if (colIdx >= children.size()) {
      continue;
    }
    auto& child = children[colIdx];
    if (child && child->encoding() == VectorEncoding::Simple::LAZY_COMPLEX) {
      child = child->asUnchecked<LazyComplexVector>()->decode(allRows, pool);
      changed = true;
    }
  }
  if (!changed) {
    return input;
  }
  return std::make_shared<RowVector>(
      input->pool(),
      input->type(),
      input->nulls(),
      input->size(),
      std::move(children));
}

namespace {
inline bool isComplexRowArrayMap(const TypePtr& type) {
  return type->isRow() || type->isArray() || type->isMap();
}
} // namespace

std::vector<InputLazyMode> makeInputLazyModes(
    size_t size,
    const std::vector<column_index_t>& channels,
    InputLazyMode mode) {
  std::vector<InputLazyMode> out(size, InputLazyMode::kAny);
  for (auto c : channels) {
    if (c < size) {
      out[c] = mode;
    }
  }
  return out;
}

RowVectorPtr applyLazyInputModes(
    const RowVectorPtr& input,
    const std::vector<InputLazyMode>& modes,
    memory::MemoryPool* pool) {
  if (!input || modes.empty()) {
    return input;
  }
  const auto* codec = LazyComplexCodec::activeCodec();
  if (codec == nullptr) {
    return input;
  }
  if (modes.size() != input->children().size()) {
    return input;
  }

  std::vector<VectorPtr> children = input->children();
  bool changed = false;
  SelectivityVector allRows(input->size());

  for (size_t i = 0; i < modes.size(); ++i) {
    auto& child = children[i];
    if (!child) {
      continue;
    }
    switch (modes[i]) {
      case InputLazyMode::kAny:
        break;
      case InputLazyMode::kForceDecoded: {
        if (child->encoding() == VectorEncoding::Simple::LAZY_COMPLEX) {
          child =
              child->asUnchecked<LazyComplexVector>()->decode(allRows, pool);
          changed = true;
        }
        break;
      }
      case InputLazyMode::kForceLazy: {
        if (isComplexRowArrayMap(child->type()) &&
            child->encoding() != VectorEncoding::Simple::LAZY_COMPLEX) {
          child = encodeToLazy(child, pool, *codec);
          changed = true;
        }
        break;
      }
    }
  }
  if (!changed) {
    return input;
  }
  return std::make_shared<RowVector>(
      input->pool(),
      input->type(),
      input->nulls(),
      input->size(),
      std::move(children));
}

RowTypePtr lazyBundleWireRowType(const RowTypePtr& type) {
  if (LazyComplexCodec::activeCodec() == nullptr) {
    return type;
  }
  bool hasComplex = false;
  std::vector<std::string> names;
  std::vector<TypePtr> children;
  names.reserve(type->size() + 1);
  children.reserve(type->size() + 1);
  for (size_t i = 0; i < type->size(); ++i) {
    const auto& child = type->childAt(i);
    if (isComplexRowArrayMap(child)) {
      hasComplex = true;
      continue;
    }
    names.push_back(type->nameOf(i));
    children.push_back(child);
  }
  if (!hasComplex) {
    return type;
  }
  constexpr const char* kLazyBundleColumnName = "__lazy_bundle__";
  names.emplace_back(kLazyBundleColumnName);
  children.emplace_back(VARBINARY());
  return ROW(std::move(names), std::move(children));
}

RowVectorPtr toLazyBundleWireRowVector(
    const RowVectorPtr& input,
    memory::MemoryPool* pool) {
  if (!input || LazyComplexCodec::activeCodec() == nullptr) {
    return input;
  }

  std::vector<const FlatVector<StringView>*> encBytes;
  std::vector<VectorPtr> nonComplexChildren;
  encBytes.reserve(input->childrenSize());
  nonComplexChildren.reserve(input->childrenSize());
  for (size_t i = 0; i < input->childrenSize(); ++i) {
    const auto& c = input->childAt(i);
    if (c && c->encoding() == VectorEncoding::Simple::LAZY_COMPLEX) {
      encBytes.push_back(c->asUnchecked<LazyComplexVector>()->encoded().get());
    } else {
      nonComplexChildren.push_back(c);
    }
  }
  if (encBytes.empty()) {
    return input;
  }

  const vector_size_t size = input->size();
  const size_t numComplex = encBytes.size();
  const size_t nullByteCount = (numComplex + 7) / 8;

  // Cache per-col StringView arrays. The invariant from
  // CompactRowLazyCodec::encode (null row => size() == 0) lets the fused
  // loop below detect nulls from len alone, so no per-col nulls pointer
  // is needed.
  std::vector<const StringView*> viewsPerCol(numComplex);
  for (size_t j = 0; j < numComplex; ++j) {
    viewsPerCol[j] = encBytes[j]->rawValues<StringView>();
  }

  // Size-only pass, column-major so each inner loop walks one col's
  // StringView array linearly (stride-16 reads, auto-vectorizable).
  const int64_t perRowOverhead = static_cast<int64_t>(nullByteCount) +
      static_cast<int64_t>(numComplex) * sizeof(uint32_t);
  int64_t total = static_cast<int64_t>(size) * perRowOverhead;
  for (size_t j = 0; j < numComplex; ++j) {
    const auto* views = viewsPerCol[j];
    int64_t colBytes = 0;
    for (vector_size_t r = 0; r < size; ++r) {
      colBytes += views[r].size();
    }
    total += colBytes;
  }

  auto arena = AlignedBuffer::allocate<char>(total > 0 ? total : 1, pool);
  auto* base = arena->asMutable<char>();
  auto valuesBuf =
      AlignedBuffer::allocate<StringView>(size > 0 ? size : 1, pool);
  auto* rawViews = valuesBuf->asMutable<StringView>();

  // Fused pass: one sequential write through the arena. Zero the per-row
  // null bitmap up-front then OR null bits directly into rowStart[j/8]
  // as we walk cols. Writing bits in place (instead of via a uint64_t
  // accumulator) keeps the path correct for any number of complex cols.
  char* p = base;
  for (vector_size_t r = 0; r < size; ++r) {
    char* const rowStart = p;
    std::memset(rowStart, 0, nullByteCount);
    p += nullByteCount;
    for (size_t j = 0; j < numComplex; ++j) {
      const auto& view = viewsPerCol[j][r];
      const uint32_t len = static_cast<uint32_t>(view.size());
      // Invariant: null iff len == 0. Bit stays 0 for non-null.
      if (len == 0) {
        rowStart[j >> 3] |= static_cast<char>(1u << (j & 7));
      }
      *reinterpret_cast<uint32_t*>(p) = len;
      p += sizeof(uint32_t);
      std::memcpy(p, view.data(), len); // no-op when len == 0
      p += len;
    }
    rawViews[r] = StringView(rowStart, static_cast<int32_t>(p - rowStart));
  }
  BOLT_DCHECK_EQ(p - base, total);

  auto bundle = std::make_shared<FlatVector<StringView>>(
      pool,
      VARBINARY(),
      /*nulls=*/nullptr,
      size,
      valuesBuf,
      std::vector<BufferPtr>{arena});

  std::vector<VectorPtr> wireChildren = std::move(nonComplexChildren);
  wireChildren.push_back(bundle);
  auto wireType = lazyBundleWireRowType(asRowType(input->type()));
  return std::make_shared<RowVector>(
      input->pool(), wireType, input->nulls(), size, std::move(wireChildren));
}

RowVectorPtr fromLazyBundleWireRowVector(
    const RowVectorPtr& wire,
    const RowTypePtr& outputType,
    memory::MemoryPool* pool) {
  if (!wire || LazyComplexCodec::activeCodec() == nullptr) {
    return wire;
  }

  std::vector<size_t> complexPositions;
  std::vector<TypePtr> complexTypes;
  for (size_t i = 0; i < outputType->size(); ++i) {
    const auto& t = outputType->childAt(i);
    if (isComplexRowArrayMap(t)) {
      complexPositions.push_back(i);
      complexTypes.push_back(t);
    }
  }
  if (complexPositions.empty()) {
    return wire;
  }

  BOLT_CHECK_GT(wire->childrenSize(), 0);
  const auto& bundleVec = wire->childAt(wire->childrenSize() - 1);
  BOLT_CHECK_EQ(bundleVec->type()->kind(), TypeKind::VARBINARY);
  auto bundle = std::dynamic_pointer_cast<FlatVector<StringView>>(bundleVec);
  BOLT_CHECK_NOT_NULL(bundle, "lazy bundle wire: bundle must be FlatVector");

  const vector_size_t size = wire->size();
  const size_t numComplex = complexPositions.size();
  const size_t nullByteCount = (numComplex + 7) / 8;

  std::vector<BufferPtr> perColValues(numComplex);
  std::vector<StringView*> perColRaw(numComplex);
  std::vector<BufferPtr> perColNulls(numComplex);
  std::vector<uint64_t*> perColRawNulls(numComplex);
  for (size_t j = 0; j < numComplex; ++j) {
    perColValues[j] =
        AlignedBuffer::allocate<StringView>(size > 0 ? size : 1, pool);
    perColRaw[j] = perColValues[j]->asMutable<StringView>();
    perColNulls[j] = AlignedBuffer::allocate<bool>(
        size > 0 ? size : 1, pool, bits::kNotNull);
    perColRawNulls[j] = perColNulls[j]->asMutable<uint64_t>();
  }

  const auto* bundleRaw = bundle->rawValues<StringView>();
  bool anyNull = false;
  for (vector_size_t r = 0; r < size; ++r) {
    if (bundle->isNullAt(r)) {
      for (size_t j = 0; j < numComplex; ++j) {
        bits::setBit(perColRawNulls[j], r, bits::kNull);
        perColRaw[j][r] = StringView();
      }
      anyNull = true;
      continue;
    }
    const auto& blob = bundleRaw[r];
    const char* const blobStart = blob.data();
    const char* p = blobStart;
    const char* end = blobStart + blob.size();
    BOLT_CHECK_LE(
        p + nullByteCount,
        end,
        "lazy bundle parse: truncated null bitmap at row {}",
        r);
    // Read null bits directly from the blob - no local buffer, so no
    // upper bound on numComplex.
    const auto* const rowNullBytes =
        reinterpret_cast<const unsigned char*>(blobStart);
    p += nullByteCount;
    // Every column contributes [len][bytes]; nulls carry len=0.
    for (size_t j = 0; j < numComplex; ++j) {
      BOLT_CHECK_LE(
          p + sizeof(uint32_t),
          end,
          "lazy bundle parse: truncated length at row {}, col {}",
          r,
          j);
      uint32_t len = 0;
      std::memcpy(&len, p, sizeof(uint32_t));
      p += sizeof(uint32_t);
      BOLT_CHECK_LE(
          p + len, end, "lazy bundle parse: truncated data at row {}", r);
      perColRaw[j][r] = StringView(p, len);
      p += len;
      if ((rowNullBytes[j >> 3] & (1u << (j & 7))) != 0) {
        bits::setBit(perColRawNulls[j], r, bits::kNull);
        anyNull = true;
      }
    }
  }

  size_t nextNonComplex = 0;
  std::vector<VectorPtr> children(outputType->size());
  for (size_t i = 0; i < outputType->size(); ++i) {
    if (std::find(complexPositions.begin(), complexPositions.end(), i) !=
        complexPositions.end()) {
      continue;
    }
    children[i] = wire->childAt(nextNonComplex++);
  }

  for (size_t j = 0; j < numComplex; ++j) {
    auto sharedBuffers = bundle->stringBuffers();
    auto colBytes = std::make_shared<FlatVector<StringView>>(
        pool,
        VARBINARY(),
        /*nulls=*/anyNull ? perColNulls[j] : nullptr,
        size,
        perColValues[j],
        std::move(sharedBuffers));
    children[complexPositions[j]] =
        std::make_shared<LazyComplexVector>(pool, complexTypes[j], colBytes);
  }

  return std::make_shared<RowVector>(
      pool, outputType, wire->nulls(), size, std::move(children));
}

namespace {
inline bool isComplexType(const TypePtr& type) {
  return type->isRow() || type->isArray() || type->isMap();
}

std::shared_ptr<LazyComplexVector> makeEmptyLazyForType(
    const TypePtr& type,
    vector_size_t size,
    memory::MemoryPool* pool) {
  // Values buffer must be non-empty even when size == 0 — StringView storage
  // requires at least one element of capacity (matches the pattern used in
  // the operator-side code).
  auto values = AlignedBuffer::allocate<StringView>(size > 0 ? size : 1, pool);
  auto flatBytes = std::make_shared<FlatVector<StringView>>(
      pool,
      VARBINARY(),
      /*nulls=*/nullptr,
      size,
      values,
      std::vector<BufferPtr>{});
  return std::make_shared<LazyComplexVector>(pool, type, flatBytes);
}
} // namespace

VectorPtr allocateLazyAwareChild(
    const TypePtr& type,
    vector_size_t size,
    memory::MemoryPool* pool) {
  if (LazyComplexCodec::activeCodec() != nullptr && isComplexType(type)) {
    return makeEmptyLazyForType(type, size, pool);
  }
  return BaseVector::create(type, size, pool);
}

RowVectorPtr allocateLazyAwareRowVector(
    const RowTypePtr& schema,
    vector_size_t size,
    memory::MemoryPool* pool) {
  return allocateLazyAwareRowVectorPrefix(schema, size, schema->size(), pool);
}

RowVectorPtr allocateLazyAwareRowVectorPrefix(
    const RowTypePtr& schema,
    vector_size_t size,
    size_t numLazyAwareCols,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> children(schema->size());
  for (size_t i = 0; i < schema->size(); ++i) {
    const auto& t = schema->childAt(i);
    children[i] = (i < numLazyAwareCols) ? allocateLazyAwareChild(t, size, pool)
                                         : BaseVector::create(t, size, pool);
  }
  return std::make_shared<RowVector>(
      pool, schema, /*nulls=*/nullptr, size, std::move(children));
}

} // namespace bytedance::bolt
