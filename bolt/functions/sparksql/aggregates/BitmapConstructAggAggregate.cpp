/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/functions/sparksql/aggregates/BitmapConstructAggAggregate.h"

#include <cstring>

#include <xsimd/xsimd.hpp>

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {

// 4096-byte inline bitmap. uint8_t ensures portable unsigned bitwise
// semantics. Trivially constructible/destructible — safe for Bolt's group
// reuse where placement-new may be called on previously freed memory.
struct BitmapAccumulator {
  uint8_t bitmap_[kBitmapNumBytes] = {};

  FOLLY_ALWAYS_INLINE void setPosition(int64_t position) {
    BOLT_USER_CHECK(
        position >= 0 && position < kBitmapNumBits,
        "Invalid bitmap position: {} (valid range: [0, {}))",
        position,
        static_cast<int64_t>(kBitmapNumBits));
    int32_t byteIdx = static_cast<int32_t>(position / 8);
    int32_t bitIdx = static_cast<int32_t>(position % 8);
    bitmap_[byteIdx] |= static_cast<uint8_t>(1 << bitIdx);
  }

  // Byte-wise bitwise OR. xsimd vectorizes across 16 (NEON) or 32 (AVX2)
  // bytes per iteration.
  void mergeWith(const char* other) {
    const auto* otherBytes = reinterpret_cast<const uint8_t*>(other);
    using Batch = xsimd::batch<uint8_t>;
    static constexpr int32_t kBatchSize = Batch::size;
    int32_t i = 0;
    for (; i + kBatchSize <= kBitmapNumBytes; i += kBatchSize) {
      auto a = Batch::load_unaligned(bitmap_ + i);
      auto b = Batch::load_unaligned(otherBytes + i);
      (a | b).store_unaligned(bitmap_ + i);
    }
    for (; i < kBitmapNumBytes; ++i) {
      bitmap_[i] |= otherBytes[i];
    }
  }
};

static_assert(
    sizeof(BitmapAccumulator) == kBitmapNumBytes,
    "BitmapAccumulator size must be exactly 4096 bytes");

// Shared raw-input decoding for single-group and multi-group paths.
template <typename GetAccumulator>
FOLLY_ALWAYS_INLINE void processRawInput(
    const DecodedVector& decoded,
    const SelectivityVector& rows,
    GetAccumulator&& getAccumulator) {
  if (decoded.isConstantMapping()) {
    if (!decoded.isNullAt(0)) {
      int64_t pos = decoded.valueAt<int64_t>(0);
      rows.applyToSelected(
          [&](vector_size_t i) { getAccumulator(i)->setPosition(pos); });
    }
  } else if (decoded.mayHaveNulls()) {
    rows.applyToSelected([&](vector_size_t row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      getAccumulator(row)->setPosition(decoded.valueAt<int64_t>(row));
    });
  } else {
    rows.applyToSelected([&](vector_size_t i) {
      getAccumulator(i)->setPosition(decoded.valueAt<int64_t>(i));
    });
  }
}

class BitmapConstructAggAggregate : public exec::Aggregate {
 public:
  explicit BitmapConstructAggAggregate(TypePtr resultType)
      : Aggregate(std::move(resultType)) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(BitmapAccumulator);
  }

  bool isFixedSize() const override {
    return true;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      new (value<BitmapAccumulator>(groups[i])) BitmapAccumulator();
    }
  }

  // ---- Raw input (BIGINT bit position) ----

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    BOLT_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);
    processRawInput(decoded, rows, [&](vector_size_t i) {
      return value<BitmapAccumulator>(groups[i]);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    BOLT_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);
    auto* accumulator = value<BitmapAccumulator>(group);
    processRawInput(decoded, rows, [accumulator](vector_size_t /*i*/) {
      return accumulator;
    });
  }

  // ---- Intermediate results (VARBINARY = serialized 4096-byte bitmap) ----

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    BOLT_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      auto sv = decoded.valueAt<StringView>(row);
      BOLT_CHECK_EQ(
          sv.size(), kBitmapNumBytes, "Intermediate bitmap must be 4096 bytes");
      value<BitmapAccumulator>(groups[row])->mergeWith(sv.data());
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    BOLT_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);
    auto* accumulator = value<BitmapAccumulator>(group);

    rows.applyToSelected([&](vector_size_t row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      auto sv = decoded.valueAt<StringView>(row);
      BOLT_CHECK_EQ(
          sv.size(), kBitmapNumBytes, "Intermediate bitmap must be 4096 bytes");
      accumulator->mergeWith(sv.data());
    });
  }

  // ---- Extraction ----

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BOLT_CHECK(result);
    auto* flatResult = (*result)->asUnchecked<FlatVector<StringView>>();
    flatResult->resize(numGroups);

    int32_t totalSize = numGroups * kBitmapNumBytes;
    char* rawBuffer = flatResult->getRawStringBufferWithSpace(totalSize);
    for (vector_size_t i = 0; i < numGroups; ++i) {
      auto* accumulator = value<BitmapAccumulator>(groups[i]);
      memcpy(rawBuffer, accumulator->bitmap_, kBitmapNumBytes);
      StringView sv(rawBuffer, kBitmapNumBytes);
      rawBuffer += kBitmapNumBytes;
      flatResult->setNoCopy(i, sv);
    }
  }

  // Identical to extractValues: accumulator format = final result format.
  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }
};

} // namespace

exec::AggregateRegistrationResult registerBitmapConstructAggAggregate(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .argumentType("bigint")
          .intermediateType("varbinary")
          .returnType("varbinary")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& /*argTypes*/,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        return std::make_unique<BitmapConstructAggAggregate>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace bytedance::bolt::functions::aggregate::sparksql
