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

#include "bolt/exec/tests/utils/SumNonPODAggregate.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/HashAggregation.h"
#include "bolt/expression/FunctionSignature.h"
namespace bytedance::bolt::exec::test {

int NonPODInt64::constructed = 0;
int NonPODInt64::destructed = 0;

namespace {

// SumNonPODAggregate uses NonPODInt64 as accumulator which has external memory
// NonPODInt64::constructed and NonPODInt64::destructed. By asserting their
// equality, we make sure Bolt calls constructor/destructor properly.
class SumNonPODAggregate : public Aggregate {
 public:
  explicit SumNonPODAggregate(bolt::TypePtr resultType, int alignment)
      : Aggregate(resultType), alignment_(alignment) {}

  int32_t accumulatorAlignmentSize() const override {
    return alignment_;
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(NonPODInt64);
  }

  bool accumulatorUsesExternalMemory() const override {
    return true;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const bolt::vector_size_t*> indices) override {
    for (auto i : indices) {
      char* group = value<char>(groups[i]);
      BOLT_CHECK_EQ(reinterpret_cast<uintptr_t>(group) % alignment_, 0);
      new (group) NonPODInt64(0);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<NonPODInt64>(group)->~NonPODInt64();
    }
  }

  void extractAccumulators(
      char** groups,
      int32_t numGroups,
      bolt::VectorPtr* result) override {
    auto vector = (*result)->as<FlatVector<int64_t>>();
    vector->resize(numGroups);
    int64_t* rawValues = vector->mutableRawValues();
    uint64_t* rawNulls = getRawNulls(vector);
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        rawValues[i] = value<NonPODInt64>(group)->value;
      }
    }
  }

  void extractValues(char** groups, int32_t numGroups, bolt::VectorPtr* result)
      override {
    extractAccumulators(groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const bolt::SelectivityVector& rows,
      const std::vector<bolt::VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      clearNull(groups[i]);
      value<NonPODInt64>(groups[i])->value += decoded.valueAt<int64_t>(i);
    });
  }

  void addRawInput(
      char** groups,
      const bolt::SelectivityVector& rows,
      const std::vector<bolt::VectorPtr>& args,
      bool mayPushdown) override {
    addIntermediateResults(groups, rows, args, mayPushdown);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const bolt::SelectivityVector& rows,
      const std::vector<bolt::VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      clearNull(group);
      value<NonPODInt64>(group)->value += decoded.valueAt<int64_t>(i);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const bolt::SelectivityVector& rows,
      const std::vector<bolt::VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupIntermediateResults(group, rows, args, mayPushdown);
  }

 private:
  const int32_t alignment_;
};

} // namespace

exec::AggregateRegistrationResult registerSumNonPODAggregate(
    const std::string& name,
    int alignment) {
  std::vector<std::shared_ptr<bolt::exec::AggregateFunctionSignature>>
      signatures{
          bolt::exec::AggregateFunctionSignatureBuilder()
              .returnType("bigint")
              .intermediateType("bigint")
              .argumentType("bigint")
              .build(),
      };

  return bolt::exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [alignment](
          bolt::core::AggregationNode::Step /*step*/,
          const std::vector<bolt::TypePtr>& /*argTypes*/,
          const bolt::TypePtr& /*resultType*/,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<bolt::exec::Aggregate> {
        return std::make_unique<SumNonPODAggregate>(bolt::BIGINT(), alignment);
      });
}

} // namespace bytedance::bolt::exec::test
