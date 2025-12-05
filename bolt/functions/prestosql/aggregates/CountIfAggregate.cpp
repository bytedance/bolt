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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include "bolt/exec/Aggregate.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SimpleVector.h"
namespace bytedance::bolt::aggregate::prestosql {

namespace {

class CountIfAggregate : public exec::Aggregate {
 public:
  explicit CountIfAggregate() : exec::Aggregate(BIGINT()) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      *value<int64_t>(groups[i]) = 0;
    }
  }

  FLATTEN void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    extractValues(groups, numGroups, result);
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    auto* vector = (*result)->as<FlatVector<int64_t>>();
    BOLT_CHECK(vector);
    vector->resize(numGroups);

    auto* rawValues = vector->mutableRawValues();
    for (vector_size_t i = 0; i < numGroups; ++i) {
      rawValues[i] = *value<int64_t>(groups[i]);
    }
  }

  FLATTEN void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    if (decoded.isConstantMapping()) {
      if (decoded.isNullAt(0)) {
        return;
      }
      if (decoded.valueAt<bool>(0)) {
        rows.applyToSelected(
            [&](vector_size_t i) { addToGroup(groups[i], 1); });
      }
    } else if (decoded.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decoded.nulls(&rows) != nullptr) {
        BOLT_CHECK(
            decoded.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decoded.size(),
                rows.end()));
        nulls = decoded.nulls(&rows);
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            if (decoded.valueAt<bool>(i)) {
              addToGroup(groups[i], 1);
            }
          },
          nulls);
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.valueAt<bool>(i)) {
          addToGroup(groups[i], 1);
        }
      });
    }
  }

  FLATTEN void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    if (decoded.isConstantMapping()) {
      auto numTrue = decoded.valueAt<int64_t>(0);
      rows.applyToSelected(
          [&](vector_size_t i) { addToGroup(groups[i], numTrue); });
      return;
    }

    rows.applyToSelected([&](vector_size_t i) {
      auto numTrue = decoded.valueAt<int64_t>(i);
      addToGroup(groups[i], numTrue);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    // Constant mapping - check once and add number of selected rows if true.
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        auto isTrue = decoded.valueAt<bool>(0);
        if (isTrue) {
          addToGroup(group, rows.countSelected());
        }
      }
      return;
    }

    int64_t numTrue = 0;
    if (decoded.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decoded.nulls(&rows) != nullptr) {
        BOLT_CHECK(
            decoded.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decoded.size(),
                rows.end()));
        nulls = decoded.nulls(&rows);
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            if (decoded.valueAt<bool>(i)) {
              ++numTrue;
            }
          },
          nulls);
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.valueAt<bool>(i)) {
          ++numTrue;
        }
      });
    }
    addToGroup(group, numTrue);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto arg = args[0]->as<SimpleVector<int64_t>>();

    int64_t numTrue = 0;
    rows.applyToSelected([&](auto row) { numTrue += arg->valueAt(row); });

    addToGroup(group, numTrue);
  }

 private:
  inline void addToGroup(char* group, int64_t numTrue) {
    *value<int64_t>(group) += numTrue;
  }
};

} // namespace

void registerCountIfAggregate(const std::string& prefix) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("bigint")
          .argumentType("boolean")
          .build(),
  };

  auto name = prefix + kCountIf;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          std::vector<TypePtr> argTypes,
          const TypePtr& /*resultType*/,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_EQ(argTypes.size(), 1, "{} takes one argument", name);

        auto isPartial = exec::isRawInput(step);
        if (isPartial) {
          BOLT_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::BOOLEAN,
              "{} function only accepts boolean parameter",
              name);
        }

        return std::make_unique<CountIfAggregate>();
      });
}

} // namespace bytedance::bolt::aggregate::prestosql
