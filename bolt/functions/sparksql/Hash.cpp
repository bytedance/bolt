/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/functions/sparksql/Hash.h"

#include <folly/CPortability.h>
#include <type/HugeInt.h>
#include <type/Type.h>
#include <vector/ComplexVector.h>
#include <cstdint>

#include "bolt/common/base/BitUtil.h"
#include "bolt/expression/DecodedArgs.h"
#include "bolt/functions/sparksql/DecimalUtil.h"
#include "bolt/type/DecimalUtil.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::functions::sparksql {
namespace {

const int32_t kDefaultSeed = 42;

// ReturnType can be either int32_t or int64_t
// HashClass contains the function like hashInt32
template <typename ReturnType, typename HashClass, typename SeedType>
void applyWithType(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
    std::optional<SeedType> seed,
    exec::EvalCtx& context,
    VectorPtr& resultRef) {
  HashClass hash;
  size_t hashIdx = seed ? 1 : 0;
  SeedType hashSeed = seed ? *seed : kDefaultSeed;

  auto& result = *resultRef->as<FlatVector<ReturnType>>();
  rows.applyToSelected([&](int row) { result.set(row, hashSeed); });

  exec::LocalSelectivityVector selectedMinusNulls(context);

  exec::DecodedArgs decodedArgs(rows, args, context);
  for (auto i = hashIdx; i < args.size(); i++) {
    auto decoded = decodedArgs.at(i);
    const SelectivityVector* selected = &rows;
    if (args[i]->mayHaveNulls()) {
      *selectedMinusNulls.get(rows.end()) = rows;
      selectedMinusNulls->deselectNulls(
          decoded->nulls(&rows), rows.begin(), rows.end());
      selected = selectedMinusNulls.get();
    }
    switch (args[i]->type()->kind()) {
// Derived from InterpretedHashFunction.hash:
// https://github.com/apache/spark/blob/382b66e/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/hash.scala#L532
#define CASE(typeEnum, hashFn, inputType)                                      \
  case TypeKind::typeEnum:                                                     \
    selected->applyToSelected([&](int row) {                                   \
      result.set(                                                              \
          row, hashFn(decoded->valueAt<inputType>(row), result.valueAt(row))); \
    });                                                                        \
    break;
      CASE(BOOLEAN, hash.hashInt32, bool);
      CASE(TINYINT, hash.hashInt32, int8_t);
      CASE(SMALLINT, hash.hashInt32, int16_t);
      CASE(INTEGER, hash.hashInt32, int32_t);
      CASE(BIGINT, hash.hashInt64, int64_t);
      CASE(VARCHAR, hash.hashBytes, StringView);
      CASE(VARBINARY, hash.hashBytes, StringView);
      CASE(REAL, hash.hashFloat, float);
      CASE(DOUBLE, hash.hashDouble, double);
      CASE(HUGEINT, hash.hashLongDecimal, int128_t);
      CASE(TIMESTAMP, hash.hashTimestamp, Timestamp);
#undef CASE
      default:
        BOLT_NYI(
            "Unsupported type for HASH(): {}", args[i]->type()->toString());
    }
  }
}

// Derived from src/main/java/org/apache/spark/unsafe/hash/Murmur3_x86_32.java.
//
// Spark's Murmur3 seems slightly different from the original from Austin
// Appleby: in particular the fmix function's first line is different. The
// original can be found here:
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
//
// Signed integer types have been remapped to unsigned types (as in the
// original) to avoid undefined signed integer overflow and sign extension.

#define SWITCH_TYPE_HASH(type, HashFunc, ...)                        \
  [&]() {                                                            \
    switch (type->kind()) {                                          \
      case TypeKind::BOOLEAN:                                        \
        return HashFunc<TypeKind::BOOLEAN>(__VA_ARGS__);             \
      case TypeKind::TINYINT:                                        \
        return HashFunc<TypeKind::TINYINT>(__VA_ARGS__);             \
      case TypeKind::SMALLINT:                                       \
        return HashFunc<TypeKind::SMALLINT>(__VA_ARGS__);            \
      case TypeKind::INTEGER:                                        \
        return HashFunc<TypeKind::INTEGER>(__VA_ARGS__);             \
      case TypeKind::BIGINT:                                         \
        return HashFunc<TypeKind::BIGINT>(__VA_ARGS__);              \
      case TypeKind::HUGEINT:                                        \
        return HashFunc<TypeKind::HUGEINT>(__VA_ARGS__);             \
      case TypeKind::VARCHAR:                                        \
        return HashFunc<TypeKind::VARCHAR>(__VA_ARGS__);             \
      case TypeKind::VARBINARY:                                      \
        return HashFunc<TypeKind::VARBINARY>(__VA_ARGS__);           \
      case TypeKind::REAL:                                           \
        return HashFunc<TypeKind::REAL>(__VA_ARGS__);                \
      case TypeKind::DOUBLE:                                         \
        return HashFunc<TypeKind::DOUBLE>(__VA_ARGS__);              \
      case TypeKind::TIMESTAMP:                                      \
        return HashFunc<TypeKind::TIMESTAMP>(__VA_ARGS__);           \
      case TypeKind::ARRAY:                                          \
        return HashFunc<TypeKind::ARRAY>(__VA_ARGS__);               \
      case TypeKind::MAP:                                            \
        return HashFunc<TypeKind::MAP>(__VA_ARGS__);                 \
      case TypeKind::ROW:                                            \
        return HashFunc<TypeKind::ROW>(__VA_ARGS__);                 \
      case TypeKind::UNKNOWN:                                        \
        return HashFunc<TypeKind::UNKNOWN>(__VA_ARGS__);             \
      default:                                                       \
        BOLT_NYI("Unsupported type for HASH: {}", type->toString()); \
    }                                                                \
  }()

template <TypeKind kind>
struct VectorValueType {
  using type = typename TypeTraits<kind>::NativeType;
};

template <>
struct VectorValueType<TypeKind::ARRAY> {
  using type = std::pair<const ArrayVector*, size_t>;
};

template <>
struct VectorValueType<TypeKind::MAP> {
  using type = std::pair<const MapVector*, size_t>;
};
template <>
struct VectorValueType<TypeKind::ROW> {
  using type = std::pair<const RowVector*, size_t>;
};

template <TypeKind kind>
typename VectorValueType<kind>::type getValueFromVector(
    const DecodedVector& vector,
    size_t index) {
  if constexpr (
      kind == TypeKind::ARRAY || kind == TypeKind::MAP ||
      kind == TypeKind::ROW) {
    typename VectorValueType<kind>::type value;
    value.first =
        vector.base()
            ->asUnchecked<std::remove_reference_t<decltype(*value.first)>>();
    value.second = vector.index(index);
    return value;
  } else {
    return vector.valueAt<typename TypeTraits<kind>::NativeType>(index);
  }
}

template <TypeKind kind>
typename VectorValueType<kind>::type getValueFromVector(
    const VectorPtr& vector,
    size_t index) {
  if constexpr (
      kind == TypeKind::ARRAY || kind == TypeKind::MAP ||
      kind == TypeKind::ROW) {
    DecodedVector decoded(*vector);
    typename VectorValueType<kind>::type value;
    value.first =
        decoded.base()
            ->asUnchecked<std::remove_reference_t<decltype(*value.first)>>();
    value.second = decoded.index(index);
    return value;
  } else {
    return vector
        ->asUnchecked<SimpleVector<typename TypeTraits<kind>::NativeType>>()
        ->valueAt(index);
  }
}

template <TypeKind kind>
typename VectorValueType<kind>::type getValueFromFlatVector(
    const BaseVector* vector,
    size_t index) {
  if constexpr (
      kind == TypeKind::ARRAY || kind == TypeKind::MAP ||
      kind == TypeKind::ROW) {
    typename VectorValueType<kind>::type value;
    assert(false);
    return value;
  } else {
    return vector
        ->asUnchecked<FlatVector<typename TypeTraits<kind>::NativeType>>()
        ->valueAt(index);
  }
}

template <TypeKind kind>
constexpr bool isIntegerType = (kind == TypeKind::BOOLEAN) ||
    (kind == TypeKind::TINYINT) || (kind == TypeKind::SMALLINT) ||
    (kind == TypeKind::INTEGER);

template <TypeKind kind>
constexpr bool isStringType = (kind == TypeKind::VARCHAR) ||
    (kind == TypeKind::VARBINARY);

class MurMur3HashBase {
 public:
  using ResultType = uint32_t;
  using SeedType = uint32_t;
  static constexpr SeedType kDefaultSeed = 42;
  static constexpr bool kIgnoreNull = true;
  static constexpr bool kSupportVectorize = false;

  static ResultType hashNull(SeedType seed) {
    return seed;
  }

 protected:
  static uint32_t mixK1(uint32_t k1) {
    k1 *= 0xcc9e2d51;
    k1 = bits::rotateLeft(k1, 15);
    k1 *= 0x1b873593;
    return k1;
  }

  static uint32_t mixH1(uint32_t h1, uint32_t k1) {
    h1 ^= k1;
    h1 = bits::rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  // Finalization mix - force all bits of a hash block to avalanche
  static uint32_t fmix(uint32_t h1, uint32_t length) {
    h1 ^= length;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    return h1;
  }

  static uint32_t hashInt32(uint32_t input, uint32_t seed) {
    uint32_t k1 = mixK1(input);
    uint32_t h1 = mixH1(seed, k1);
    return fmix(h1, 4);
  }

  static uint32_t hashInt64(uint64_t input, uint32_t seed) {
    uint32_t low = input;
    uint32_t high = input >> 32;

    uint32_t k1 = mixK1(low);
    uint32_t h1 = mixH1(seed, k1);

    k1 = mixK1(high);
    h1 = mixH1(h1, k1);

    return fmix(h1, 8);
  }

  // Spark also has an hashUnsafeBytes2 function, but it was not used at the
  // time of implementation.
  static uint32_t hashBytes(const StringView& input, uint32_t seed) {
    const char* i = input.data();
    const char* const end = input.data() + input.size();
    uint32_t h1 = seed;
    for (; i <= end - 4; i += 4) {
      h1 = mixH1(h1, mixK1(*reinterpret_cast<const uint32_t*>(i)));
    }
    for (; i != end; ++i) {
      h1 = mixH1(h1, mixK1(*i));
    }
    return fmix(h1, input.size());
  }
};

class XxHash64Base {
 public:
  using ResultType = uint64_t;
  using SeedType = uint64_t;
  static constexpr SeedType kDefaultSeed = 42;
  static constexpr bool kIgnoreNull = true;
  static constexpr bool kSupportVectorize = false;

  static ResultType hashNull(SeedType seed) {
    return seed;
  }

 private:
  static constexpr uint64_t PRIME64_1 = 0x9E3779B185EBCA87L;
  static constexpr uint64_t PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  static constexpr uint64_t PRIME64_3 = 0x165667B19E3779F9L;
  static constexpr uint64_t PRIME64_4 = 0x85EBCA77C2B2AE63L;
  static constexpr uint64_t PRIME64_5 = 0x27D4EB2F165667C5L;

 protected:
  static ResultType hashInt32(const int32_t input, uint64_t seed) {
    int64_t hash = seed + PRIME64_5 + 4L;
    hash ^= static_cast<int64_t>((input & 0xFFFFFFFFL) * PRIME64_1);
    hash = bits::rotateLeft64(hash, 23) * PRIME64_2 + PRIME64_3;
    return fmix(hash);
  }

  static ResultType hashInt64(int64_t input, uint64_t seed) {
    int64_t hash = seed + PRIME64_5 + 8L;
    hash ^= bits::rotateLeft64(input * PRIME64_2, 31) * PRIME64_1;
    hash = bits::rotateLeft64(hash, 27) * PRIME64_1 + PRIME64_4;
    return fmix(hash);
  }

  static ResultType hashBytes(const StringView& input, uint64_t seed) {
    const char* i = input.data();
    const char* const end = input.data() + input.size();

    uint64_t hash = hashBytesByWords(input, seed);
    uint32_t length = input.size();
    auto offset = i + (length & -8);
    if (offset + 4L <= end) {
      hash ^= (*reinterpret_cast<const uint64_t*>(offset) & 0xFFFFFFFFL) *
          PRIME64_1;
      hash = bits::rotateLeft64(hash, 23) * PRIME64_2 + PRIME64_3;
      offset += 4L;
    }

    while (offset < end) {
      hash ^= (*reinterpret_cast<const uint64_t*>(offset) & 0xFFL) * PRIME64_5;
      hash = bits::rotateLeft64(hash, 11) * PRIME64_1;
      offset++;
    }
    return fmix(hash);
  }

 private:
  static uint64_t fmix(uint64_t hash) {
    hash ^= hash >> 33;
    hash *= PRIME64_2;
    hash ^= hash >> 29;
    hash *= PRIME64_3;
    hash ^= hash >> 32;
    return hash;
  }

  static uint64_t hashBytesByWords(const StringView& input, uint64_t seed) {
    const char* i = input.data();
    const char* const end = input.data() + input.size();
    uint32_t length = input.size();
    uint64_t hash;
    if (length >= 32) {
      uint64_t v1 = seed + PRIME64_1 + PRIME64_2;
      uint64_t v2 = seed + PRIME64_2;
      uint64_t v3 = seed;
      uint64_t v4 = seed - PRIME64_1;
      for (; i <= end - 32; i += 32) {
        v1 = bits::rotateLeft64(
                 v1 + (*reinterpret_cast<const uint64_t*>(i) * PRIME64_2), 31) *
            PRIME64_1;
        v2 = bits::rotateLeft64(
                 v2 + (*reinterpret_cast<const uint64_t*>(i + 8) * PRIME64_2),
                 31) *
            PRIME64_1;
        v3 = bits::rotateLeft64(
                 v3 + (*reinterpret_cast<const uint64_t*>(i + 16) * PRIME64_2),
                 31) *
            PRIME64_1;
        v4 = bits::rotateLeft64(
                 v4 + (*reinterpret_cast<const uint64_t*>(i + 24) * PRIME64_2),
                 31) *
            PRIME64_1;
      }
      hash = bits::rotateLeft64(v1, 1) + bits::rotateLeft64(v2, 7) +
          bits::rotateLeft64(v3, 12) + bits::rotateLeft64(v4, 18);
      v1 *= PRIME64_2;
      v1 = bits::rotateLeft64(v1, 31);
      v1 *= PRIME64_1;
      hash ^= v1;
      hash = hash * PRIME64_1 + PRIME64_4;

      v2 *= PRIME64_2;
      v2 = bits::rotateLeft64(v2, 31);
      v2 *= PRIME64_1;
      hash ^= v2;
      hash = hash * PRIME64_1 + PRIME64_4;

      v3 *= PRIME64_2;
      v3 = bits::rotateLeft64(v3, 31);
      v3 *= PRIME64_1;
      hash ^= v3;
      hash = hash * PRIME64_1 + PRIME64_4;

      v4 *= PRIME64_2;
      v4 = bits::rotateLeft64(v4, 31);
      v4 *= PRIME64_1;
      hash ^= v4;
      hash = hash * PRIME64_1 + PRIME64_4;
    } else {
      hash = seed + PRIME64_5;
    }

    hash += length;

    for (; i <= end - 8; i += 8) {
      hash ^= bits::rotateLeft64(
                  *reinterpret_cast<const uint64_t*>(i) * PRIME64_2, 31) *
          PRIME64_1;
      hash = bits::rotateLeft64(hash, 27) * PRIME64_1 + PRIME64_4;
    }
    return hash;
  }
};

template <TypeKind kind, typename HashBase>
class NonHiveHash : public HashBase {
  using NativeType = typename VectorValueType<kind>::type;
  using ResultType = typename HashBase::ResultType;
  using SeedType = typename HashBase::SeedType;

 public:
  NonHiveHash(const TypePtr& type) {}

  inline ResultType hash(const NativeType& input, SeedType seed) const {
    if constexpr (isIntegerType<kind>) {
      return HashBase::hashInt32((uint32_t)input, seed);
    } else if constexpr (kind == TypeKind::BIGINT) {
      return HashBase::hashInt64((uint64_t)input, seed);
    } else if constexpr (kind == TypeKind::REAL) {
      return HashBase::hashInt32(
          input == -0.f ? 0 : *reinterpret_cast<const uint32_t*>(&input), seed);
    } else if constexpr (kind == TypeKind::DOUBLE) {
      return HashBase::hashInt64(
          input == -0. ? 0 : *reinterpret_cast<const uint64_t*>(&input), seed);
    } else if constexpr (isStringType<kind>) {
      return HashBase::hashBytes(input, seed);
    } else if constexpr (kind == TypeKind::HUGEINT) {
      char out[sizeof(int128_t)];
      int32_t length = bolt::DecimalUtil::toByteArray(input, out);
      return HashBase::hashBytes(StringView(out, length), seed);
    } else if constexpr (kind == TypeKind::TIMESTAMP) {
      return HashBase::hashInt64(input.toMicros(), seed);
    } else if constexpr (kind == TypeKind::ARRAY) {
      auto array = std::get<0>(input);
      auto index = std::get<1>(input);
      return SWITCH_TYPE_HASH(
          array->elements()->type(), hashArrayElement, array, index, seed);
    } else if constexpr (kind == TypeKind::MAP) {
      auto mapVector = std::get<0>(input);
      auto index = std::get<1>(input);
      ResultType result = seed;
      if (mapVector->isNullAt(index)) {
        return HashBase::hashNull(seed);
      }
      auto start = mapVector->offsetAt(index);
      auto end = start + mapVector->sizeAt(index);
      for (auto idx = start; idx < end; idx++) {
        BOLT_DCHECK(!mapVector->mapKeys()->isNullAt(idx));
        result = SWITCH_TYPE_HASH(
            mapVector->mapKeys()->type(),
            hashOne,
            mapVector->mapKeys(),
            idx,
            result);
        result = SWITCH_TYPE_HASH(
            mapVector->mapValues()->type(),
            hashOne,
            mapVector->mapValues(),
            idx,
            result);
      }
      return result;
    } else if constexpr (kind == TypeKind::ROW) {
      auto rowVector = std::get<0>(input);
      auto index = std::get<1>(input);
      ResultType result = seed;
      if (rowVector->isNullAt(index)) {
        return HashBase::hashNull(result);
      }
      for (const auto& child : rowVector->children()) {
        result = SWITCH_TYPE_HASH(child->type(), hashOne, child, index, result);
      }
      return result;
    } else if constexpr (kind == TypeKind::UNKNOWN) {
      return HashBase::hashNull(seed);
    } else {
      BOLT_UNREACHABLE("unsupported hash type");
    }
  }

 private:
  template <TypeKind elementKind>
  static ResultType
  hashArrayElement(const ArrayVector* array, size_t index, SeedType seed) {
    ResultType result = seed;
    NonHiveHash<elementKind, HashBase> hasher(array->elements()->type());
    if (array->isNullAt(index)) {
      return HashBase::hashNull(seed);
    }
    auto start = array->offsetAt(index);
    auto end = start + array->sizeAt(index);
    for (auto idx = start; idx < end; idx++) {
      if (array->elements()->isNullAt(idx)) {
        continue;
      }
      result = hasher.hash(
          getValueFromVector<elementKind>(array->elements(), idx), result);
    }
    return result;
  }

  template <TypeKind elementKind>
  static ResultType
  hashOne(const VectorPtr& baseVector, size_t index, SeedType seed) {
    NonHiveHash<elementKind, HashBase> hasher(baseVector->type());
    if (baseVector->isNullAt(index)) {
      return HashBase::hashNull(seed);
    } else {
      return hasher.hash(
          getValueFromVector<elementKind>(baseVector, index), seed);
    }
  }
};

template <TypeKind kind>
using Murmur3Hash = NonHiveHash<kind, MurMur3HashBase>;

template <TypeKind kind>
using XxHash64 = NonHiveHash<kind, XxHash64Base>;

class HiveHashBase {
 public:
  using ResultType = int32_t;
  using SeedType = int32_t;
  static constexpr bool kIgnoreNull = false;
  static constexpr SeedType kDefaultSeed = 0;
  static constexpr bool kSupportVectorize = true;

  static ResultType hashNull(SeedType seed) {
    return genSeed(seed);
  }

  static inline int32_t genSeed(SeedType seed) {
    return 31 * seed;
  }

 protected:
  static ResultType hashInt32(int32_t input, SeedType seed) {
    return genSeed(seed) + input;
  }

  static ResultType hashInt64(int64_t input, SeedType seed) {
    return genSeed(seed) +
        (input ^ static_cast<uint32_t>(static_cast<uint64_t>(input) >> 32));
  }

  template <typename InputType>
  static uint8_t normalizeDecimal(InputType& input, uint8_t scale) {
    auto trimDecimal = [](InputType& input, uint8_t& scale) {
      functions::sparksql::DecimalUtil::stripTrailingZeros(input, scale);
    };
    trimDecimal(input, scale);
    return scale;
  }

  template <typename InputType>
  static ResultType decimalHashCode(InputType input, uint8_t scale) {
    return 31 * hashBigInteger(input) + scale;
  }

  template <typename InputType>
  static ResultType hashBigInteger(InputType val) {
    constexpr uint8_t magSize = sizeof(InputType) / sizeof(int32_t);
    uint8_t magLen = 1;
    int32_t mag[magSize];
    int32_t sign = 0;

    if (val > 0) {
      sign = 1;
    } else {
      val = -val;
      sign = -1;
    }
    InputType oldVal = val;
    while ((oldVal >> 32) != 0) {
      magLen++;
      oldVal >>= 32;
    }

    for (int i = magLen - 1; i >= 0; i--) {
      mag[i] = (int32_t)val;
      val >>= 32;
    }

    int32_t hashCode = 0;
    for (auto i = 0; i < magLen; ++i) {
      hashCode = (int32_t)(31 * hashCode + (mag[i] & 0xffffffffL));
    }
    return sign * hashCode;
  }
};

template <TypeKind kind>
class HiveHash : public HiveHashBase {
 public:
  using NativeType = typename VectorValueType<kind>::type;
  using ResultType = HiveHashBase::ResultType;
  using SeedType = HiveHashBase::SeedType;
  static ResultType hashNull(SeedType seed) {
    return seed;
  }

  HiveHash(const TypePtr& type) {}

  inline ResultType hash(const NativeType& input, SeedType seed) const {
    // note: BIGINT may store decimal value, so we handle it in specification
    if constexpr (isIntegerType<kind>) {
      return HiveHashBase::hashInt32((uint32_t)input, seed);
    } else if constexpr (kind == TypeKind::REAL) {
      if (input == -0.0f) {
        return HiveHashBase::genSeed(seed);
      }
      if (input != input) { // Java isNaN
        return HiveHashBase::genSeed(seed) + 0x7fc00000;
      }

      union {
        int32_t i;
        float f;
      } u;

      u.f = input;
      return HiveHashBase::genSeed(seed) + u.i;
    } else if constexpr (kind == TypeKind::DOUBLE) {
      return hashInt64(
          input == -0. ? 0 : *reinterpret_cast<const uint64_t*>(&input), seed);
    } else if constexpr (isStringType<kind>) {
      int32_t result = 0;
      size_t start = 0, end = input.size();
#ifdef __x86_64__
      // clang-format off
      // hash(s[0:n]) = hash(s[0:n-1]) * 31 + s[n-1]
      //              = s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-2]*31 + s[n-1]
      // if n = 8k then
      // hash(s[0:8k]) = s[0]*31^(8k-1) + s[1]*31^(8k-2) + ... + s[8k-2]*31 + s[8k-1]
      //               = s[0]*31^(8k-1) + s[8]*31^(8k-9) + ... + s[8k-8]*31^7 +
      //                 s[1]*31^(8k-2) + s[9]*31^(8k-10) + ... + s[8k-7]*31^6 +
      //                 ... +
      //                 s[7]*31^(8k-8) + s[15]*31^(8k-16) + ... + s[8k-1]
      //               = (s[0]*31^(8k-8) + s[8]*31^(8k-16) + ... + s[8k-8]) * 31^7 +
      //                 (s[1]*31^(8k-8) + s[9]*31^(8k-16) + ... + s[8k-7]) * 31^6 +
      //                 ... +
      //                 (s[7]*31^(8k-8) + s[15]*31^(8k-16) + ... + s[8k-1])
      // we define <s[0], [s1], ..., s[7]> as a vector, . as dot product, we can get
      // hash(s[0:8k]) = (<s[0], s[1], ..., s[7]> * 31^(8k-8) +
      //                  <s[8], s[9], ..., s[15]> * 31^(8k-16) +
      //                  ... +
      //                  <s[8k-8], s[8k-7], ..., s[8k-1]>)
      //                  . (dot product) <31^7, 31^6, ..., 31, 1>
      // we can easily compute this formula with SIMD instructions,
      // first we set temp result a 8xi32 0 value, then for each 8 char
      // we store it into a 8xi32, then multiple temp result with 31^8,
      // after that we add new temp result with the 8xi32 chars.
      // all char is computed, we do a dot product on temp result and
      // <31^7, 31^6, ..., 31, 1> and get final temp result
      // for rest of char less than 8, do normal hash with final temp result as seed
      //
      // note: multiple is difficult for SIMD, so we transfor 8xi32*31^8 with simple add and shift.
      // 31^8 = 0b 1100 0110 1001 0100 0100 0100 0110 1111 0000 0001
      // we only care about the lower 32 bits, cause result is i32
      // so a * 31^8 = a * 0b 1001 0100 0100 0100 0110 1111 0000 0001
      //             = a * (1 << 31 + 1 << 28 + 1 << 26 + 1 << 22 + 1 << 18 + 1111111 << 8 - 1 << 12 + 1)
      //             = a * (1 << 31 + 1 << 28 + 1 << 26 + 1 << 22 + 1 << 18 + 1 << 15 - 1 << 8 - 1 << 12 + 1)
      //             = a * ((1 << 3 + 1) << 28 + (1 << 4 + 1) << 22 + (1 << 3 + 1) << 15 - (1 << 4 + 1) << 8 + 1)
      // clang-format on
      if (start + 8 <= end) {
        __m64 value = *(__m64*)(input.data() + start);
        __m256i temp = _mm256_cvtepi8_epi32(_mm_set1_epi64(value));
        start += 8;
        while (start + 8 <= end) {
          __m64 value = *(__m64*)(input.data() + start);
          __m256i extended = _mm256_cvtepi8_epi32(_mm_set1_epi64(value));
          // temp = temp * 31^8
          __m256i s3 = _mm256_slli_epi32(temp, 3);
          __m256i s4 = _mm256_slli_epi32(temp, 4);
          __m256i sl3 = _mm256_add_epi32(s3, temp);
          __m256i sl4 = _mm256_add_epi32(s4, temp);
          __m256i middle = _mm256_add_epi32(
              _mm256_add_epi32(
                  _mm256_slli_epi32(sl3, 28), _mm256_slli_epi32(sl4, 22)),
              _mm256_sub_epi32(
                  _mm256_slli_epi32(sl3, 15), _mm256_slli_epi32(sl4, 8)));
          temp = _mm256_add_epi32(temp, extended);
          temp = _mm256_add_epi32(temp, middle);
          start += 8;
        }
        __m128i upper = _mm256_extractf128_si256(temp, 0);
        __m128i lower = _mm256_extractf128_si256(temp, 1);
        auto compute4 = [](__m128i tempVec, __m128i newVec) {
          // 31^4 = 0b 1110 0001 0111 1000 0001
          //      = 1110000001 << 10 + 1110000001 + 1 << 12
          //
          // 1110000001 = 1 + (1 << 3 - 1) << 7 = 1 + 1 << 10 - 1 << 7
          __m128i middle = _mm_sub_epi32(
              _mm_add_epi32(tempVec, _mm_slli_epi32(tempVec, 10)),
              _mm_slli_epi32(tempVec, 7));
          tempVec = _mm_add_epi32(
              _mm_slli_epi32(middle, 10),
              _mm_add_epi32(middle, _mm_slli_epi32(tempVec, 12)));
          return _mm_add_epi32(tempVec, newVec);
        };
        upper = compute4(upper, lower);
        if (start + 4 <= end) {
          lower =
              _mm_cvtepi8_epi32(_mm_set1_epi32(*(int*)(input.data() + start)));
          upper = compute4(upper, lower);
          start += 4;
        }
        result = _mm_extract_epi32(upper, 0);
        result = result * 31 + _mm_extract_epi32(upper, 1);
        result = result * 31 + _mm_extract_epi32(upper, 2);
        result = result * 31 + _mm_extract_epi32(upper, 3);
      }
#endif
      while (start < end) {
        result = result * 31 + (ResultType)(input.data()[start++]);
      }
      return HiveHashBase::genSeed(seed) + result;
    } else if constexpr (kind == TypeKind::TIMESTAMP) {
      int64_t seconds = input.getSeconds();
      // spark timestamp only support microseconds, so clean nanoseconds part
      // before hash
      int64_t nanos = input.getNanos() / Timestamp::kNanosecondsInMicrosecond *
          Timestamp::kNanosecondsInMicrosecond;

      int64_t result = seconds;
      result <<= 30;
      result |= nanos;
      return hashInt64(result, seed);
    } else if constexpr (kind == TypeKind::ARRAY) {
      auto array = std::get<0>(input);
      auto index = std::get<1>(input);
      ResultType result = HiveHashBase::genSeed(seed);
      result += SWITCH_TYPE_HASH(
          array->elements()->type(), hashArrayElement, array, index, 0);
      return result;
    } else if constexpr (kind == TypeKind::MAP) {
      auto mapVector = std::get<0>(input);
      auto index = std::get<1>(input);
      ResultType result = seed;
      if (mapVector->isNullAt(index)) {
        return HiveHashBase::hashNull(result);
      }
      result = 0;
      auto start = mapVector->offsetAt(index);
      auto end = start + mapVector->sizeAt(index);
      for (auto idx = start; idx < end; idx++) {
        BOLT_DCHECK(!mapVector->mapKeys()->isNullAt(idx));
        ResultType keyHash = SWITCH_TYPE_HASH(
            mapVector->mapKeys()->type(),
            hashOne,
            mapVector->mapKeys(),
            idx,
            0);
        ResultType valueHash = SWITCH_TYPE_HASH(
            mapVector->mapValues()->type(),
            hashOne,
            mapVector->mapValues(),
            idx,
            0);
        result += keyHash ^ valueHash;
      }
      return HiveHashBase::genSeed(seed) + result;
    } else if constexpr (kind == TypeKind::ROW) {
      auto rowVector = std::get<0>(input);
      auto index = std::get<1>(input);
      ResultType result = seed;
      if (rowVector->isNullAt(index)) {
        return HiveHashBase::hashNull(result);
      }
      result = 0;
      auto childrenSize = rowVector->childrenSize();
      for (auto child : rowVector->children()) {
        result = SWITCH_TYPE_HASH(child->type(), hashOne, child, index, result);
      }
      return HiveHashBase::genSeed(seed) + result;
    } else if constexpr (kind == TypeKind::UNKNOWN) {
      return HiveHashBase::hashNull(seed);
    } else {
      BOLT_UNREACHABLE("unsupported hash type");
    }
  }

 private:
  template <TypeKind elementKind>
  static ResultType
  hashArrayElement(const ArrayVector* array, size_t index, SeedType seed) {
    ResultType result = seed;
    HiveHash<elementKind> hasher(array->elements()->type());
    if (array->isNullAt(index)) {
      return HiveHashBase::hashNull(seed);
    }
    auto start = array->offsetAt(index);
    auto end = start + array->sizeAt(index);
    for (auto idx = start; idx < end; idx++) {
      if (array->elements()->isNullAt(idx)) {
        result = HiveHashBase::hashNull(result);
      } else {
        result = hasher.hash(
            getValueFromVector<elementKind>(array->elements(), idx), result);
      }
    }
    return result;
  }

  template <TypeKind elementKind>
  static ResultType
  hashOne(const VectorPtr& baseVector, size_t index, SeedType seed) {
    HiveHash<elementKind> hasher(baseVector->type());
    if (baseVector->isNullAt(index)) {
      return HiveHashBase::hashNull(seed);
    } else {
      return hasher.hash(
          getValueFromVector<elementKind>(baseVector, index), seed);
    }
  }
};

template <>
class HiveHash<TypeKind::BIGINT> : HiveHashBase {
 public:
  using NativeType = TypeTraits<TypeKind::BIGINT>::NativeType;

  HiveHash(const TypePtr& type)
      : isDecimal_(type->isShortDecimal()),
        scale_(isDecimal_ ? type->asShortDecimal().scale() : 0) {}

  ResultType hash(const NativeType& input, SeedType seed) const {
    if (FOLLY_UNLIKELY(isDecimal_)) {
      if (input == 0) {
        return genSeed(seed);
      }
      NativeType value = input;
      uint8_t newScale = normalizeDecimal(value, scale_);
      return genSeed(seed) + decimalHashCode(value, newScale);
    } else {
      return HiveHashBase::hashInt64((uint64_t)input, seed);
    }
  }

 private:
  const bool isDecimal_;
  const uint8_t scale_;
};

template <>
class HiveHash<TypeKind::HUGEINT> : HiveHashBase {
 public:
  using NativeType = TypeTraits<TypeKind::HUGEINT>::NativeType;

  HiveHash(const TypePtr& type) : scale_(type->asLongDecimal().scale()) {}

  ResultType hash(const NativeType& input, SeedType seed) const {
    if (input == 0) {
      return genSeed(seed);
    }
    NativeType value = input;
    uint8_t newScale = normalizeDecimal(value, scale_);
    return genSeed(seed) + decimalHashCode(value, newScale);
  }

 private:
  int8_t scale_;
};

// hash multiple values from input into result with seed, if seed is nullptr,
// use default seed 0. this function is optimized for no null vector, so that
// compiler can generate SIMD instructions. we also optimized for complex type
// with vectorized hash for it's element
template <TypeKind typeKind>
__attribute__((noinline)) bool hiveHashMultiple(
    const BaseVector* input,
    HiveHashBase::ResultType* result,
    size_t inputSize,
    bool useDefaultSeed) {
  if constexpr (TypeTraits<typeKind>::isPrimitiveType) {
    HiveHash<typeKind> hasher(input->type());
    if constexpr (typeKind != TypeKind::BOOLEAN) {
      if (input->isFlatEncoding()) {
        auto flatVector =
            input->asFlatVector<typename TypeTraits<typeKind>::NativeType>();
        bool hasNoNull = flatVector->rawNulls()
            ? bits::isAllSet(
                  flatVector->rawNulls(), 0, inputSize, bits::kNotNull)
            : true;
        if (hasNoNull) {
          auto rawInput = flatVector->rawValues();
          for (size_t row = 0; row < inputSize; row++) {
            result[row] =
                hasher.hash(rawInput[row], useDefaultSeed ? 0 : result[row]);
          }
        } else {
          for (size_t row = 0; row < inputSize; row++) {
            HiveHashBase::ResultType hashValue;
            if (flatVector->isNullAt(row)) {
              hashValue =
                  HiveHashBase::hashNull(useDefaultSeed ? 0 : result[row]);
            } else {
              hashValue = hasher.hash(
                  flatVector->valueAtFast(row),
                  useDefaultSeed ? 0 : result[row]);
            }
            result[row] = hashValue;
          }
        }
        return true;
      }
    }
    DecodedVector decoded(*input);
    for (size_t i = 0; i < inputSize; i++) {
      HiveHashBase::SeedType seed = useDefaultSeed ? 0 : result[i];
      HiveHashBase::ResultType hashValue = 0;
      if (decoded.isNullAt(i)) {
        hashValue = HiveHashBase::hashNull(seed);
      } else {
        hashValue = hasher.hash(
            decoded.valueAt<typename TypeTraits<typeKind>::NativeType>(i),
            seed);
      }
      result[i] = hashValue;
    }
  } else if constexpr (typeKind == TypeKind::ARRAY) {
    DecodedVector decoded(*input);
    const ArrayVector* arrayVector = decoded.base()->as<const ArrayVector>();
    std::vector<HiveHashBase::ResultType> partialResult(
        arrayVector->elements()->size());
    SWITCH_TYPE_HASH(
        arrayVector->elements()->type(),
        hiveHashMultiple,
        arrayVector->elements().get(),
        partialResult.data(),
        partialResult.size(),
        true);
    for (size_t i = 0; i < inputSize; i++) {
      HiveHashBase::SeedType seed = useDefaultSeed ? 0 : result[i];
      HiveHashBase::ResultType hashValue = 0;
      if (decoded.isNullAt(i)) {
        hashValue = HiveHashBase::hashNull(seed);
      } else {
        auto index = decoded.index(i);
        auto start = arrayVector->offsetAt(index);
        auto end = start + arrayVector->sizeAt(index);
        HiveHashBase::ResultType tempResult = 0;
        for (auto idx = start; idx < end; idx++) {
          tempResult = HiveHashBase::genSeed(tempResult) + partialResult[idx];
        }
        hashValue = HiveHashBase::genSeed(seed) + tempResult;
      }
      result[i] = hashValue;
    }
  } else if constexpr (typeKind == TypeKind::MAP) {
    DecodedVector decoded(*input);
    const MapVector* mapVector = decoded.base()->as<const MapVector>();
    std::vector<HiveHashBase::ResultType> keyResult(
        mapVector->mapKeys()->size());
    SWITCH_TYPE_HASH(
        mapVector->mapKeys()->type(),
        hiveHashMultiple,
        mapVector->mapKeys().get(),
        keyResult.data(),
        keyResult.size(),
        true);
    std::vector<HiveHashBase::ResultType> valueResult(
        mapVector->mapValues()->size());
    SWITCH_TYPE_HASH(
        mapVector->mapValues()->type(),
        hiveHashMultiple,
        mapVector->mapValues().get(),
        valueResult.data(),
        valueResult.size(),
        true);
    for (size_t i = 0; i < inputSize; i++) {
      HiveHashBase::SeedType seed = useDefaultSeed ? 0 : result[i];
      HiveHashBase::ResultType hashValue = 0;
      if (decoded.isNullAt(i)) {
        hashValue = HiveHashBase::hashNull(seed);
      } else {
        auto index = decoded.index(i);
        auto start = mapVector->offsetAt(index);
        auto end = start + mapVector->sizeAt(index);
        HiveHashBase::ResultType tempResult = 0;
        for (auto idx = start; idx < end; idx++) {
          tempResult += keyResult[idx] ^ valueResult[idx];
        }
        hashValue = HiveHashBase::genSeed(seed) + tempResult;
      }
      result[i] = hashValue;
    }
  } else if constexpr (typeKind == TypeKind::ROW) {
    DecodedVector decoded(*input);
    const RowVector* rowVector = decoded.base()->as<const RowVector>();
    std::vector<HiveHashBase::ResultType> partialResult(rowVector->size(), 0);
    for (const auto& child : rowVector->children()) {
      SWITCH_TYPE_HASH(
          child->type(),
          hiveHashMultiple,
          child.get(),
          partialResult.data(),
          partialResult.size(),
          false);
    }
    for (size_t i = 0; i < inputSize; i++) {
      HiveHashBase::SeedType seed = useDefaultSeed ? 0 : result[i];
      HiveHashBase::ResultType hashValue = 0;
      if (decoded.isNullAt(i)) {
        hashValue = HiveHashBase::hashNull(seed);
      } else {
        hashValue =
            HiveHashBase::genSeed(seed) + partialResult[decoded.index(i)];
      }
      result[i] = hashValue;
    }
  } else if constexpr (typeKind == TypeKind::UNKNOWN) {
    for (size_t i = 0; i < inputSize; i++) {
      HiveHashBase::SeedType seed = useDefaultSeed ? 0 : result[i];
      HiveHashBase::ResultType hashValue = 0;
      hashValue = HiveHashBase::hashNull(seed);
      result[i] = hashValue;
    }
  } else {
    BOLT_UNREACHABLE("unsupported hash type");
  }
  return true;
}

template <template <TypeKind> typename HashClass, typename HashClassBase>
struct HashFunctionEvaluator {
  // change type to signed as all data in flat vector is signed type
  using ResultType = std::make_signed_t<typename HashClassBase::ResultType>;

  template <TypeKind kind>
  static __attribute__((noinline)) void hashNonNullValues(
      const HashClass<kind>& hasher,
      size_t inputSize,
      const typename TypeTraits<kind>::NativeType* rawInput,
      ResultType* resultValues) {
    for (size_t row = 0; row < inputSize; row++) {
      resultValues[row] = hasher.hash(rawInput[row], resultValues[row]);
    }
  }

  template <TypeKind kind>
  static bool hashOneColumn(
      const SelectivityVector& rows,
      const VectorPtr& input,
      FlatVector<ResultType>& result) {
    auto rawValues = result.mutableRawValues();
    if (rows.isAllSelected()) {
      if constexpr (std::is_same_v<HashClassBase, HiveHashBase>) {
        hiveHashMultiple<kind>(input.get(), rawValues, rows.size(), false);
        return true;
      }
    }
    HashClass<kind> hasher(input->type());
    DecodedVector decoded(*input);
    rows.applyToSelected([&](int row) {
      ResultType hashValue;
      if (decoded.isNullAt(row)) {
        hashValue = HashClassBase::hashNull(rawValues[row]);
      } else {
        hashValue =
            hasher.hash(getValueFromVector<kind>(decoded, row), rawValues[row]);
      }
      rawValues[row] = hashValue;
    });
    return true;
  }

  static void apply(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>&
          args, // Not using const ref so we can reuse args
      std::optional<typename HashClassBase::SeedType> seed,
      exec::EvalCtx& context,
      VectorPtr& resultRef) {
    size_t hashIdx = seed ? 1 : 0;
    auto hashSeed = seed ? *seed : HashClassBase::kDefaultSeed;

    auto& result = *resultRef->as<FlatVector<ResultType>>();
    auto rawValues = result.mutableRawValues();
    result.clearNulls(rows);
    rows.applyToSelected([&](int row) { rawValues[row] = hashSeed; });

    for (auto i = hashIdx; i < args.size(); i++) {
      SWITCH_TYPE_HASH(args[i]->type(), hashOneColumn, rows, args[i], result);
    }
    if constexpr (std::is_same_v<HashClassBase, HiveHashBase>) {
      rows.applyToSelected([&](int row) { rawValues[row] &= 0x7FFFFFFF; });
    }
  }
};

class Murmur3HashFunction final : public exec::VectorFunction {
 public:
  Murmur3HashFunction() = default;
  explicit Murmur3HashFunction(int32_t seed) : seed_(seed) {}

  bool isDefaultNullBehavior() const final {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    context.ensureWritable(rows, INTEGER(), resultRef);
    HashFunctionEvaluator<Murmur3Hash, MurMur3HashBase>::apply(
        rows, args, seed_, context, resultRef);
  }

 private:
  const std::optional<int32_t> seed_;
};

class XxHash64Function final : public exec::VectorFunction {
 public:
  XxHash64Function() = default;
  explicit XxHash64Function(int64_t seed) : seed_(seed) {}

  bool isDefaultNullBehavior() const final {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    context.ensureWritable(rows, BIGINT(), resultRef);
    HashFunctionEvaluator<XxHash64, XxHash64Base>::apply(
        rows, args, seed_, context, resultRef);
  }

 private:
  const std::optional<int64_t> seed_;
};

class HiveHashFunction final : public exec::VectorFunction {
 public:
  HiveHashFunction() = default;
  explicit HiveHashFunction(int32_t seed) : seed_(seed) {}

  bool isDefaultNullBehavior() const final {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    context.ensureWritable(rows, INTEGER(), resultRef);
    HashFunctionEvaluator<HiveHash, HiveHashBase>::apply(
        rows, args, seed_, context, resultRef);
  }

 private:
  const std::optional<int32_t> seed_;
};

} // namespace

// Not all types are supported by now. Check types when making hash function.
// See checkArgTypes.
std::vector<std::shared_ptr<exec::FunctionSignature>> hashSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("integer")
              .argumentType("any")
              .variableArity()
              .build()};
}

void checkArgTypes(const std::vector<exec::VectorFunctionArg>& args) {
  for (const auto& arg : args) {
    switch (arg.type->kind()) {
      case TypeKind::BOOLEAN:
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
      case TypeKind::REAL:
      case TypeKind::DOUBLE:
      case TypeKind::HUGEINT:
      case TypeKind::TIMESTAMP:
      case TypeKind::ARRAY:
      case TypeKind::ROW:
      case TypeKind::MAP:
      case TypeKind::UNKNOWN:
        break;
      default:
        BOLT_USER_FAIL("Unsupported type for hash: {}", arg.type->toString())
    }
  }
}

std::shared_ptr<exec::VectorFunction> makeHash(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  checkArgTypes(inputArgs);
  static const auto kHashFunction = std::make_shared<Murmur3HashFunction>();
  return kHashFunction;
}

std::shared_ptr<exec::VectorFunction> makeHashWithSeed(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  checkArgTypes(inputArgs);
  const auto& constantSeed = inputArgs[0].constantValue;
  if (!constantSeed || constantSeed->isNullAt(0)) {
    BOLT_USER_FAIL("{} requires a constant non-null seed argument.", name);
  }
  auto seed = constantSeed->as<ConstantVector<int32_t>>()->valueAt(0);
  return std::make_shared<Murmur3HashFunction>(seed);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> hashWithSeedSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("integer")
              .constantArgumentType("integer")
              .argumentType("any")
              .variableArity()
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> xxhash64Signatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType("any")
              .variableArity()
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
xxhash64WithSeedSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .constantArgumentType("bigint")
              .argumentType("any")
              .variableArity()
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeXxHash64(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  checkArgTypes(inputArgs);
  static const auto kXxHash64Function = std::make_shared<XxHash64Function>();
  return kXxHash64Function;
}

std::shared_ptr<exec::VectorFunction> makeXxHash64WithSeed(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  const auto& constantSeed = inputArgs[0].constantValue;
  if (!constantSeed || constantSeed->isNullAt(0)) {
    BOLT_USER_FAIL("{} requires a constant non-null seed argument.", name);
  }
  auto seed = constantSeed->as<ConstantVector<int64_t>>()->valueAt(0);
  return std::make_shared<XxHash64Function>(seed);
}

std::shared_ptr<exec::VectorFunction> makeHiveHash(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  static const auto kHiveHashFunction = std::make_shared<HiveHashFunction>();
  return kHiveHashFunction;
}

} // namespace bytedance::bolt::functions::sparksql
