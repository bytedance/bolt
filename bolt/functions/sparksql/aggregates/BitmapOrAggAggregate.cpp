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

#include "bolt/functions/sparksql/aggregates/BitmapOrAggAggregate.h"

#include <cstring>

#include "bolt/exec/Aggregate.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/sparksql/aggregates/BitmapUtil.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {

// Shared varbinary-input decoding for single-group and multi-group paths.
template <typename GetAccumulator>
FOLLY_ALWAYS_INLINE void processBinaryInput(
    const DecodedVector& decoded,
    const SelectivityVector& rows,
    GetAccumulator&& getAccumulator) {
  if (decoded.isConstantMapping()) {
    if (!decoded.isNullAt(0)) {
      auto sv = decoded.valueAt<StringView>(0);
      BOLT_CHECK_EQ(
          sv.size(), kBitmapNumBytes, "Input bitmap must be 4096 bytes");
      rows.applyToSelected(
          [&](vector_size_t i) { getAccumulator(i)->mergeWith(sv.data()); });
    }
  } else if (decoded.mayHaveNulls()) {
    rows.applyToSelected([&](vector_size_t row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      auto sv = decoded.valueAt<StringView>(row);
      BOLT_CHECK_EQ(
          sv.size(), kBitmapNumBytes, "Input bitmap must be 4096 bytes");
      getAccumulator(row)->mergeWith(sv.data());
    });
  } else {
    rows.applyToSelected([&](vector_size_t i) {
      auto sv = decoded.valueAt<StringView>(i);
      BOLT_CHECK_EQ(
          sv.size(), kBitmapNumBytes, "Input bitmap must be 4096 bytes");
      getAccumulator(i)->mergeWith(sv.data());
    });
  }
}

class BitmapOrAggAggregate : public exec::Aggregate {
 public:
  explicit BitmapOrAggAggregate(TypePtr resultType)
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

  // ---- Raw input (VARBINARY = 4096-byte bitmap) ----

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    BOLT_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);
    processBinaryInput(decoded, rows, [&](vector_size_t i) {
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
    processBinaryInput(decoded, rows, [accumulator](vector_size_t /*i*/) {
      return accumulator;
    });
  }

  // ---- Intermediate results (same as raw input: VARBINARY → mergeWith) ----

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    BOLT_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);
    processBinaryInput(decoded, rows, [&](vector_size_t i) {
      return value<BitmapAccumulator>(groups[i]);
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
    processBinaryInput(decoded, rows, [accumulator](vector_size_t /*i*/) {
      return accumulator;
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

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }
};

} // namespace

exec::AggregateRegistrationResult registerBitmapOrAggAggregate(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .argumentType("varbinary")
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
        return std::make_unique<BitmapOrAggAggregate>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace bytedance::bolt::functions::aggregate::sparksql
