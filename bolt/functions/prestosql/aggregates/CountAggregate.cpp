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

#include "bolt/common/base/Exceptions.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/functions/lib/aggregates/AggregateToIntermediate.h"
#include "bolt/functions/lib/aggregates/SumAggregateBase.h"
#include "bolt/functions/prestosql/aggregates/AggregateNames.h"
using namespace bytedance::bolt::functions::aggregate;
namespace bytedance::bolt::aggregate::prestosql {

namespace {

template <bool presto_non_null_count = false>
class CountAggregate : public SimpleNumericAggregate<bool, int64_t, int64_t> {
  using BaseAggregate = SimpleNumericAggregate<bool, int64_t, int64_t>;

 public:
  explicit CountAggregate() : BaseAggregate(BIGINT()) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  bool supportsToIntermediate() const override {
    return true;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    if (args.empty()) {
      BOLT_CHECK(result->typeKind() == TypeKind::BIGINT);
      auto resultValues = result->asFlatVector<int64_t>();
      auto rawNulls = result->mutableRawNulls();
      bits::setAllNull(rawNulls, result->size());
      rows.applyToSelected([&](vector_size_t i) { resultValues->set(i, 1); });
      return;
    }
    countToIntermediate(rows, args, result);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      // result of count is never null
      *value<int64_t>(groups[i]) = (int64_t)0;
    }
  }

  FLATTEN void
  extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    BaseAggregate::doExtractValues(groups, numGroups, result, [&](char* group) {
      return *value<int64_t>(group);
    });
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (args.empty()) {
      rows.applyToSelected([&](vector_size_t i) { addToGroup(groups[i], 1); });
      return;
    }

    DecodedVector decoded(*args[0], rows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        rows.applyToSelected(
            [&](vector_size_t i) { addToGroup(groups[i], 1); });
      }
    } else if (
        presto_non_null_count &&
        decoded.base()->encoding() == VectorEncoding::Simple::ROW) {
      const auto* rowVector = decoded.base()->as<RowVector>();
      rows.applyToSelected([&](vector_size_t row) {
        if (decoded.isNullAt(row)) {
          return;
        }
        int i = decoded.index(row);
        for (auto child = 0; child < rowVector->childrenSize(); ++child) {
          if (rowVector->childAt(child)->isNullAt(i)) {
            return;
          }
        }
        addToGroup(groups[row], 1);
      });
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
          [&](vector_size_t i) { addToGroup(groups[i], 1); }, nulls);
    } else {
      rows.applyToSelected([&](vector_size_t i) { addToGroup(groups[i], 1); });
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t i) {
      addToGroup(groups[i], decodedIntermediate_.valueAt<int64_t>(i));
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (args.empty()) {
      addToGroup(group, rows.countSelected());
      return;
    }

    DecodedVector decoded(*args[0], rows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        addToGroup(group, rows.countSelected());
      }
    } else if (
        presto_non_null_count &&
        decoded.base()->encoding() == VectorEncoding::Simple::ROW) {
      int64_t nonNullCount = 0;
      const auto* rowVector = decoded.base()->as<RowVector>();
      rows.applyToSelected([&](vector_size_t row) {
        if (decoded.isNullAt(row)) {
          return;
        }
        int i = decoded.index(row);
        for (auto child = 0; child < rowVector->childrenSize(); ++child) {
          if (rowVector->childAt(child)->isNullAt(i)) {
            return;
          }
        }
        ++nonNullCount;
      });
      addToGroup(group, nonNullCount);
    } else if (decoded.mayHaveNulls()) {
      int64_t nonNullCount = 0;
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
      rows.applyToSelected([&](vector_size_t i) { ++nonNullCount; }, nulls);
      addToGroup(group, nonNullCount);
    } else {
      addToGroup(group, rows.countSelected());
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);

    int64_t count = 0;
    if (decodedIntermediate_.mayHaveNulls()) {
      const uint64_t* nulls = nullptr;
      if (decodedIntermediate_.nulls() != nullptr) {
        BOLT_CHECK(
            decodedIntermediate_.size() == rows.end(),
            fmt::format(
                "decoded.size() {}!= rows.end() {}",
                decodedIntermediate_.size(),
                rows.end()));
        nulls = decodedIntermediate_.nulls();
      }
      rows.applyToSelected(
          [&](vector_size_t i) {
            count += decodedIntermediate_.valueAt<int64_t>(i);
          },
          nulls);
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        count += decodedIntermediate_.valueAt<int64_t>(i);
      });
    }

    addToGroup(group, count);
  }

 private:
  inline void addToGroup(char* group, int64_t count) {
    *value<int64_t>(group) += count;
  }

  DecodedVector decodedIntermediate_;
};

} // namespace

void registerCountAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("bigint")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .intermediateType("bigint")
          .argumentType("T")
          .build(),
  };

  auto name = prefix + kCount;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& /*resultType*/,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        return std::make_unique<CountAggregate<>>();
      },
      withCompanionFunctions,
      overwrite);

  name = prefix + "non_null_count";
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& /*resultType*/,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_EQ(
            argTypes.size(), 1, "non_null_count takes only one argument");
        BOLT_CHECK_EQ(
            argTypes[0]->kind(),
            TypeKind::ROW,
            "non_null_count can only be used on row type");
        return std::make_unique<CountAggregate<true>>();
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace bytedance::bolt::aggregate::prestosql
