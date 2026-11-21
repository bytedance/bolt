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

#pragma once

#include <algorithm>
#include <functional>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "bolt/shuffle/sparksql/tests/CellTestUtils.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/LazyVector.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

/// Nested input for the Cell shuffle tests: vectors of any Cell type from a
/// seed, the encodings a batch's columns come in, and the schemas the
/// round trips and the golden bytes share.
namespace bytedance::bolt::shuffle::sparksql::cell::test {

/// How a top-level column holds its rows in a batch.
enum class ColumnEncoding {
  kFlat,
  kDictionary,
  kConstant,
  kConstantNull,
  kSlice,
  kLazyIdentity,
  kLazyDictionary,
  kCount,
};

/// `vector` behind a LazyVector that loads it as a dictionary over it.
inline VectorPtr lazyDictionary(VectorPtr vector, memory::MemoryPool* pool) {
  const auto size = vector->size();
  auto type = vector->type();
  return std::make_shared<LazyVector>(
      pool,
      std::move(type),
      size,
      std::make_unique<bolt::test::SimpleVectorLoader>(
          [vector = std::move(vector), pool](RowSet /*rows*/) {
            auto indices = allocateIndices(vector->size(), pool);
            auto* raw = indices->asMutable<vector_size_t>();
            std::iota(raw, raw + vector->size(), 0);
            return BaseVector::wrapInDictionary(
                nullptr, std::move(indices), vector->size(), vector);
          }));
}

/// Vectors of any Cell type, nested to any depth, from a seed: nulls at
/// each level at a rate of its own, empty containers, null containers that
/// keep their elements, containers anywhere in their elements, map keys in
/// no order, and dictionaries, constants and slices inside the containers.
class NestedVectorMaker {
 public:
  struct Options {
    /// Percent of null rows at each nesting level, the top one first; the
    /// last applies below as well.
    std::vector<uint32_t> nullPercent{10};
    /// A container holds up to this many elements.
    vector_size_t maxLength{4};
    /// Percent of the containers that are empty.
    uint32_t emptyPercent{20};
    /// Percent of the null containers that keep a size, their elements
    /// still in the elements vector.
    uint32_t sizedNullPercent{50};
    /// Percent of the containers placed anywhere in their elements:
    /// overlapping one another and out of order.
    uint32_t scatteredPercent{0};
    /// Percent of the vectors below the top level that come wrapped: in a
    /// dictionary with nulls of its own, a constant, or a slice; map keys,
    /// in a dictionary that reorders them.
    uint32_t wrapPercent{0};
  };

  NestedVectorMaker(memory::MemoryPool* pool, uint64_t seed, Options options)
      : pool_(pool), rng_(seed), options_(std::move(options)) {}

  /// `size` rows of `type` at nesting `level`.
  VectorPtr make(const TypePtr& type, vector_size_t size, uint32_t level = 0) {
    switch (type->kind()) {
      case TypeKind::ARRAY:
        return makeArray(type, size, level);
      case TypeKind::MAP:
        return makeMap(type, size, level);
      case TypeKind::ROW:
        return makeRow(type, size, level);
      default:
        return makeLeaf(type, size, level);
    }
  }

  /// A top-level column of `type`, `size` rows, held as `encoding`, over
  /// vectors from `makeValues`, by default make().
  VectorPtr column(
      const TypePtr& type,
      vector_size_t size,
      ColumnEncoding encoding,
      const std::function<VectorPtr(vector_size_t)>& makeValues = nullptr) {
    const auto values = [&](vector_size_t n) {
      return makeValues ? makeValues(n) : make(type, n);
    };
    switch (encoding) {
      case ColumnEncoding::kFlat:
        return values(size);
      case ColumnEncoding::kDictionary:
      case ColumnEncoding::kLazyDictionary: {
        const vector_size_t baseSize = size / 2 + 1;
        auto base = values(baseSize);
        auto indices = allocateIndices(size, pool_);
        auto* raw = indices->asMutable<vector_size_t>();
        for (vector_size_t row = 0; row < size; ++row) {
          raw[row] = static_cast<vector_size_t>(rng_() % baseSize);
        }
        auto nulls = AlignedBuffer::allocate<bool>(size, pool_, bits::kNotNull);
        for (vector_size_t row = 0; row < size; ++row) {
          if (rng_() % 10 == 0) {
            bits::setNull(nulls->asMutable<uint64_t>(), row);
          }
        }
        auto dictionary = BaseVector::wrapInDictionary(
            std::move(nulls), std::move(indices), size, std::move(base));
        return encoding == ColumnEncoding::kLazyDictionary
            ? lazyDictionary(std::move(dictionary), pool_)
            : dictionary;
      }
      case ColumnEncoding::kConstant: {
        auto base = values(4);
        return BaseVector::wrapInConstant(
            size, static_cast<vector_size_t>(rng_() % 4), std::move(base));
      }
      case ColumnEncoding::kConstantNull:
        return BaseVector::createNullConstant(type, size, pool_);
      case ColumnEncoding::kSlice:
        return values(size + 20)->slice(7, size);
      case ColumnEncoding::kLazyIdentity:
        return lazyDictionary(values(size), pool_);
      case ColumnEncoding::kCount:
        break;
    }
    BOLT_UNREACHABLE();
  }

  std::mt19937_64& rng() {
    return rng_;
  }

 private:
  bool chance(uint32_t percent) {
    return rng_() % 100 < percent;
  }

  BufferPtr nulls(vector_size_t size, uint32_t level) {
    const auto& rates = options_.nullPercent;
    const auto percent = rates[std::min<size_t>(level, rates.size() - 1)];
    if (percent == 0) {
      return nullptr;
    }
    auto nulls = AlignedBuffer::allocate<bool>(size, pool_, bits::kNotNull);
    auto* raw = nulls->asMutable<uint64_t>();
    for (vector_size_t row = 0; row < size; ++row) {
      if (chance(percent)) {
        bits::setNull(raw, row);
      }
    }
    return nulls;
  }

  /// A vector below the top level: made as is, or wrapped.
  VectorPtr child(const TypePtr& type, vector_size_t size, uint32_t level) {
    if (size == 0 || !chance(options_.wrapPercent)) {
      return make(type, size, level);
    }
    switch (rng_() % 3) {
      case 0: {
        const vector_size_t baseSize = size / 2 + 1;
        auto base = make(type, baseSize, level);
        auto indices = allocateIndices(size, pool_);
        auto* raw = indices->asMutable<vector_size_t>();
        for (vector_size_t row = 0; row < size; ++row) {
          raw[row] = static_cast<vector_size_t>(rng_() % baseSize);
        }
        return BaseVector::wrapInDictionary(
            nulls(size, level), std::move(indices), size, std::move(base));
      }
      case 1: {
        auto base = make(type, 3, level);
        return BaseVector::wrapInConstant(
            size, static_cast<vector_size_t>(rng_() % 3), std::move(base));
      }
      default:
        return make(type, size + 10, level)->slice(5, size);
    }
  }

  /// Offsets and sizes of `size` containers, their nulls given; returns
  /// the size of their elements vector.
  vector_size_t containers(
      vector_size_t size,
      const BufferPtr& entryNulls,
      BufferPtr& offsets,
      BufferPtr& sizes) {
    std::vector<vector_size_t> lengths(size);
    vector_size_t total = 0;
    for (vector_size_t row = 0; row < size; ++row) {
      const bool isNull = entryNulls != nullptr &&
          bits::isBitNull(entryNulls->as<uint64_t>(), row);
      if ((isNull && !chance(options_.sizedNullPercent)) ||
          options_.maxLength == 0 || chance(options_.emptyPercent)) {
        lengths[row] = 0;
      } else {
        lengths[row] =
            1 + static_cast<vector_size_t>(rng_() % options_.maxLength);
      }
      total += lengths[row];
    }
    offsets = allocateOffsets(size, pool_);
    sizes = allocateSizes(size, pool_);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    vector_size_t at = 0;
    for (vector_size_t row = 0; row < size; ++row) {
      rawSizes[row] = lengths[row];
      rawOffsets[row] = at;
      if (chance(options_.scatteredPercent)) {
        rawOffsets[row] =
            static_cast<vector_size_t>(rng_() % (total - lengths[row] + 1));
      }
      at += lengths[row];
    }
    return total;
  }

  VectorPtr makeArray(const TypePtr& type, vector_size_t size, uint32_t level) {
    auto entryNulls = nulls(size, level);
    BufferPtr offsets;
    BufferPtr sizes;
    const auto total = containers(size, entryNulls, offsets, sizes);
    auto elements = child(type->childAt(0), total, level + 1);
    return std::make_shared<ArrayVector>(
        pool_,
        type,
        std::move(entryNulls),
        size,
        std::move(offsets),
        std::move(sizes),
        std::move(elements));
  }

  VectorPtr makeMap(const TypePtr& type, vector_size_t size, uint32_t level) {
    auto entryNulls = nulls(size, level);
    BufferPtr offsets;
    BufferPtr sizes;
    const auto total = containers(size, entryNulls, offsets, sizes);
    auto keys = makeKeys(type->childAt(0), total, level + 1);
    auto values = child(type->childAt(1), total, level + 1);
    return std::make_shared<MapVector>(
        pool_,
        type,
        std::move(entryNulls),
        size,
        std::move(offsets),
        std::move(sizes),
        std::move(keys),
        std::move(values));
  }

  /// 0 to `size` - 1 in random order.
  std::vector<vector_size_t> permutation(vector_size_t size) {
    std::vector<vector_size_t> order(size);
    for (vector_size_t i = 0; i < size; ++i) {
      const auto j = static_cast<vector_size_t>(rng_() % (i + 1));
      order[i] = order[j];
      order[j] = i;
    }
    return order;
  }

  /// Keys never null and all different, so that no map holds one twice
  /// wherever its entries lie, in no order: a map's entries keep theirs
  /// only if the shuffle keeps it. A struct key differs by its first field,
  /// its others any value at nesting `level`.
  VectorPtr makeKeys(const TypePtr& type, vector_size_t size, uint32_t level) {
    const auto base = static_cast<int64_t>(rng_() % 1'000'000);
    const auto order = permutation(size);
    VectorPtr keys;
    switch (type->kind()) {
      case TypeKind::INTEGER: {
        auto flat = BaseVector::create<FlatVector<int32_t>>(type, size, pool_);
        for (vector_size_t row = 0; row < size; ++row) {
          flat->set(row, static_cast<int32_t>(base + order[row]));
        }
        keys = std::move(flat);
        break;
      }
      case TypeKind::BIGINT: {
        auto flat = BaseVector::create<FlatVector<int64_t>>(type, size, pool_);
        for (vector_size_t row = 0; row < size; ++row) {
          flat->set(row, (base + order[row]) * 7919);
        }
        keys = std::move(flat);
        break;
      }
      case TypeKind::VARCHAR: {
        auto flat =
            BaseVector::create<FlatVector<StringView>>(type, size, pool_);
        for (vector_size_t row = 0; row < size; ++row) {
          // Some inline, some not.
          const auto key = fmt::format(
              "{}{}",
              base + order[row],
              std::string(rng_() % 2 == 0 ? 2 : 20, 'k'));
          flat->set(row, StringView(key));
        }
        keys = std::move(flat);
        break;
      }
      case TypeKind::ROW: {
        std::vector<VectorPtr> fields{makeKeys(type->childAt(0), size, level)};
        for (uint32_t i = 1; i < type->size(); ++i) {
          fields.push_back(make(type->childAt(i), size, level));
        }
        keys = std::make_shared<RowVector>(
            pool_, type, nullptr, size, std::move(fields));
        break;
      }
      default:
        BOLT_FAIL("no map keys of {}", type->toString());
    }
    if (size == 0 || !chance(options_.wrapPercent)) {
      return keys;
    }
    // A dictionary of distinct indices keeps the keys distinct.
    const auto shuffled = permutation(size);
    auto indices = allocateIndices(size, pool_);
    std::copy(
        shuffled.begin(), shuffled.end(), indices->asMutable<vector_size_t>());
    return BaseVector::wrapInDictionary(
        nullptr, std::move(indices), size, std::move(keys));
  }

  VectorPtr makeRow(const TypePtr& type, vector_size_t size, uint32_t level) {
    auto rowNulls = nulls(size, level);
    std::vector<VectorPtr> fields;
    for (uint32_t i = 0; i < type->size(); ++i) {
      fields.push_back(child(type->childAt(i), size, level + 1));
    }
    return std::make_shared<RowVector>(
        pool_, type, std::move(rowNulls), size, std::move(fields));
  }

  template <typename T>
  VectorPtr flat(
      const TypePtr& type,
      vector_size_t size,
      const std::function<T()>& value) {
    auto vector = BaseVector::create<FlatVector<T>>(type, size, pool_);
    for (vector_size_t row = 0; row < size; ++row) {
      vector->set(row, value());
    }
    return vector;
  }

  VectorPtr leafValues(const TypePtr& type, vector_size_t size) {
    auto& rng = rng_;
    switch (type->kind()) {
      case TypeKind::BOOLEAN:
        return flat<bool>(type, size, [&] { return (rng() & 1) == 1; });
      case TypeKind::TINYINT:
        return flat<int8_t>(
            type, size, [&] { return static_cast<int8_t>(rng()); });
      case TypeKind::SMALLINT:
        return flat<int16_t>(
            type, size, [&] { return static_cast<int16_t>(rng()); });
      case TypeKind::INTEGER:
        return flat<int32_t>(type, size, [&] {
          return type->isDate() ? static_cast<int32_t>(rng() % 40'000)
                                : static_cast<int32_t>(rng());
        });
      case TypeKind::BIGINT:
        return flat<int64_t>(type, size, [&] {
          return type->isDecimal()
              ? static_cast<int64_t>(rng() % 19'999'999'999) - 9'999'999'999
              : static_cast<int64_t>(rng());
        });
      case TypeKind::HUGEINT:
        return flat<int128_t>(type, size, [&] {
          const auto high = static_cast<int64_t>(rng() % 1'000'000'000'000);
          const auto low = static_cast<int64_t>(rng() % 1'000'000'000'000);
          return static_cast<int128_t>(high) * 1'000'000'000'000 - low;
        });
      case TypeKind::REAL:
        return flat<float>(type, size, [&] {
          return static_cast<float>(static_cast<int32_t>(rng() % 100'000)) / 8;
        });
      case TypeKind::DOUBLE:
        return flat<double>(type, size, [&] {
          return static_cast<double>(static_cast<int64_t>(rng() % 1'000'000)) /
              64;
        });
      case TypeKind::TIMESTAMP:
        // Microseconds; nanoTimestamps() gives nanosecond values.
        return flat<Timestamp>(type, size, [&] {
          const auto seconds =
              static_cast<int64_t>(rng() % 4'000'000'000) - 2'000'000'000;
          const auto micros = rng() % 1'000'000;
          return Timestamp(seconds, micros * 1000);
        });
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        std::string value;
        return flat<StringView>(type, size, [&] {
          const size_t length = rng() % 8 == 0 ? 50 + rng() % 200 : rng() % 16;
          value.resize(length);
          for (auto& c : value) {
            c = static_cast<char>('a' + rng() % 26);
          }
          return StringView(value);
        });
      }
      default:
        BOLT_FAIL("no values of {}", type->toString());
    }
  }

  VectorPtr makeLeaf(const TypePtr& type, vector_size_t size, uint32_t level) {
    if (type->kind() == TypeKind::UNKNOWN) {
      return BaseVector::createNullConstant(type, size, pool_);
    }
    auto vector = leafValues(type, size);
    vector->setNulls(nulls(size, level));
    return vector;
  }

  memory::MemoryPool* const pool_;
  std::mt19937_64 rng_;
  const Options options_;
};

/// One batch per entry of `rows`, of that many rows of `type`, from
/// `options` and `seed`, spread over `numPartitions`; the columns of batch
/// b are held as `encodingOf(b, column)`.
inline ShuffleInput makeInput(
    const RowTypePtr& type,
    NestedVectorMaker::Options options,
    const std::vector<vector_size_t>& rows,
    int32_t numPartitions,
    uint64_t seed,
    memory::MemoryPool* pool,
    const std::function<ColumnEncoding(uint32_t, uint32_t)>& encodingOf =
        nullptr) {
  NestedVectorMaker maker(pool, seed, std::move(options));
  ShuffleInput input;
  for (uint32_t batch = 0; batch < rows.size(); ++batch) {
    std::vector<VectorPtr> children;
    for (uint32_t col = 0; col < type->size(); ++col) {
      children.push_back(maker.column(
          type->childAt(col),
          rows[batch],
          encodingOf ? encodingOf(batch, col) : ColumnEncoding::kFlat));
    }
    input.data.push_back(std::make_shared<RowVector>(
        pool, type, nullptr, rows[batch], std::move(children)));
    auto& pids = input.pids.emplace_back(rows[batch]);
    for (auto& pid : pids) {
      pid = static_cast<int32_t>(maker.rng()() % numPartitions);
    }
  }
  return input;
}

/// Every type a nested column can hold at its leaves.
inline std::vector<TypePtr> leafTypes() {
  return {
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      HUGEINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
      DECIMAL(10, 2),
      DECIMAL(30, 3),
      UNKNOWN()};
}

/// Whether V1 carries `type`: all but a bare HUGEINT, which no Arrow type
/// describes.
inline bool v1Carries(const TypePtr& type) {
  if (type->kind() == TypeKind::HUGEINT && !type->isDecimal()) {
    return false;
  }
  for (uint32_t i = 0; i < type->size(); ++i) {
    if (!v1Carries(type->childAt(i))) {
      return false;
    }
  }
  return true;
}

/// Nested columns four levels deep, and one flat column beside them.
inline RowTypePtr deepRowType() {
  const auto point = ROW({"x", "y"}, {INTEGER(), TIMESTAMP()});
  return ROW(
      {"id", "arrays", "maps", "rows", "pointMaps", "mixed", "nestedMaps"},
      {BIGINT(),
       ARRAY(ARRAY(ARRAY(ARRAY(SMALLINT())))),
       MAP(VARCHAR(), ARRAY(MAP(INTEGER(), point))),
       ROW({"a"},
           {ROW(
               {"b"}, {ROW({"c"}, {ROW({"d", "e"}, {BIGINT(), VARCHAR()})})})}),
       ARRAY(MAP(VARCHAR(), ARRAY(point))),
       ROW({"arr", "n"},
           {ARRAY(ROW({"m"}, {MAP(BIGINT(), VARCHAR())})), INTEGER()}),
       MAP(BIGINT(), MAP(VARCHAR(), MAP(INTEGER(), DOUBLE())))});
}

/// Arrays and maps four levels deep with no ROW at any depth, beside a
/// BIGINT and a VARCHAR column.
inline RowTypePtr deepNestedType() {
  return ROW(
      {"id", "arrays", "maps", "mixed", "nestedMaps", "s"},
      {BIGINT(),
       ARRAY(ARRAY(ARRAY(ARRAY(SMALLINT())))),
       MAP(VARCHAR(), ARRAY(MAP(INTEGER(), TIMESTAMP()))),
       ARRAY(MAP(VARCHAR(), ARRAY(DOUBLE()))),
       MAP(BIGINT(), MAP(VARCHAR(), MAP(INTEGER(), DOUBLE()))),
       VARCHAR()});
}

/// The offsets of `count` containers of `length` elements each, back to
/// back.
inline std::vector<vector_size_t> evenOffsets(
    vector_size_t count,
    vector_size_t length) {
  std::vector<vector_size_t> offsets(count);
  for (vector_size_t i = 0; i < count; ++i) {
    offsets[i] = i * length;
  }
  return offsets;
}

/// Timestamps to the nanosecond, at and around the epoch and far from it,
/// then 3000 more from `seed`.
inline std::vector<Timestamp> nanoTimestamps(uint64_t seed) {
  std::vector<Timestamp> values = {
      Timestamp(0, 1),
      Timestamp(-1, 999'999'999),
      Timestamp(1'700'000'000, 123'456'789),
      Timestamp(-62'135'596'800, 7)};
  std::mt19937_64 rng(seed);
  for (int i = 0; i < 3000; ++i) {
    const auto seconds =
        static_cast<int64_t>(rng() % 200'000'000'000) - 100'000'000'000;
    values.push_back(Timestamp(seconds, rng() % 1'000'000'000));
  }
  return values;
}

/// Arrays of `elements`, every one starting at element 0: row r holds the
/// first `lengths[r]`. With `map`, maps whose keys and values are both
/// `elements`.
inline VectorPtr containersOf(
    const std::vector<vector_size_t>& lengths,
    const VectorPtr& elements,
    memory::MemoryPool* pool,
    bool map = false) {
  const auto rows = static_cast<vector_size_t>(lengths.size());
  auto offsets = allocateOffsets(rows, pool);
  auto sizes = allocateSizes(rows, pool);
  std::copy(lengths.begin(), lengths.end(), sizes->asMutable<vector_size_t>());
  const auto& type = elements->type();
  if (map) {
    return std::make_shared<MapVector>(
        pool,
        MAP(type, type),
        nullptr,
        rows,
        offsets,
        sizes,
        elements,
        elements);
  }
  return std::make_shared<ArrayVector>(
      pool, ARRAY(type), nullptr, rows, offsets, sizes, elements);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell::test
