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

#include "bolt/functions/sparksql/aggregates/CollectListAggregate.h"

#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/exec/SimpleAggregateAdapter.h"
#include "bolt/functions/lib/aggregates/ValueList.h"
using namespace bytedance::bolt::aggregate;
using namespace bytedance::bolt::exec;
namespace bytedance::bolt::functions::aggregate::sparksql {
namespace {
class CollectListAggregate {
 public:
  using InputType = Row<Generic<T1>>;

  using IntermediateType = Array<Generic<T1>>;

  using OutputType = Array<Generic<T1>>;

  /// In Spark, when all inputs are null, the output is an empty array instead
  /// of null. Therefore, in the writeIntermediateResult and writeFinalResult,
  /// we still need to output the empty element_ when the group is null. This
  /// behavior can only be achieved when the default-null behavior is disabled.
  static constexpr bool default_null_behavior_ = false;

  static bool toIntermediate(
      exec::out_type<Array<Generic<T1>>>& out,
      exec::optional_arg_type<Generic<T1>> in) {
    if (in.has_value()) {
      out.add_item().copy_from(in.value());
      return true;
    }
    return false;
  }

  struct AccumulatorType {
    ValueList elements_;

    explicit AccumulatorType(
        HashStringAllocator* /*allocator*/,
        CollectListAggregate* /*fn*/)
        : elements_{} {}

    static constexpr bool is_fixed_size_ = false;

    bool addInput(
        HashStringAllocator* allocator,
        exec::optional_arg_type<Generic<T1>> data) {
      if (data.has_value()) {
        elements_.appendValue(data, allocator);
        return true;
      }
      return false;
    }

    bool combine(
        HashStringAllocator* allocator,
        exec::optional_arg_type<IntermediateType> other) {
      if (!other.has_value()) {
        return false;
      }
      for (auto element : other.value()) {
        elements_.appendValue(element, allocator);
      }
      return true;
    }

    bool writeIntermediateResult(
        bool /*nonNullGroup*/,
        exec::out_type<IntermediateType>& out) {
      // If the group's accumulator is null, the corresponding intermediate
      // result is an empty array.
      copyValueListToArrayWriter(out, elements_);
      return true;
    }

    bool writeFinalResult(
        bool /*nonNullGroup*/,
        exec::out_type<OutputType>& out) {
      // If the group's accumulator is null, the corresponding result is an
      // empty array.
      copyValueListToArrayWriter(out, elements_);
      return true;
    }

    void destroy(HashStringAllocator* allocator) {
      elements_.free(allocator);
    }
  };
};

template <typename T>
class TypedCollectListAggregate {
 public:
  using InputType = Row<T>;

  using IntermediateType = Array<T>;

  using OutputType = Array<T>;

  static constexpr bool default_null_behavior_ = false;

  static bool toIntermediate(
      exec::out_type<Array<T>>& out,
      exec::optional_arg_type<T> in) {
    if (in.has_value()) {
      out.push_back(in.value());
      return true;
    }
    return false;
  }

  struct AccumulatorType {
    using Values = std::vector<T, StlAllocator<T>>;

    Values elements_;

    explicit AccumulatorType(
        HashStringAllocator* allocator,
        TypedCollectListAggregate* /*fn*/)
        : elements_{StlAllocator<T>(allocator)} {}

    static constexpr bool is_fixed_size_ = false;

    bool addInput(
        HashStringAllocator* /*allocator*/,
        exec::optional_arg_type<T> data) {
      if (data.has_value()) {
        elements_.push_back(data.value());
        return true;
      }
      return false;
    }

    bool combine(
        HashStringAllocator* /*allocator*/,
        exec::optional_arg_type<IntermediateType> other) {
      if (!other.has_value()) {
        return false;
      }
      const auto values = other.value();
      elements_.reserve(elements_.size() + values.size());
      for (auto element : values.skipNulls()) {
        elements_.push_back(element);
      }
      return true;
    }

    bool writeIntermediateResult(
        bool /*nonNullGroup*/,
        exec::out_type<IntermediateType>& out) {
      copyToArrayWriter(out);
      return true;
    }

    bool writeFinalResult(
        bool /*nonNullGroup*/,
        exec::out_type<OutputType>& out) {
      copyToArrayWriter(out);
      return true;
    }

   private:
    template <typename TArrayWriter>
    void copyToArrayWriter(TArrayWriter& out) {
      out.resetLength();
      if (elements_.empty()) {
        return;
      }
      out.reserve(elements_.size());
      for (const auto& value : elements_) {
        out.push_back(value);
      }
    }
  };
};

template <typename T>
std::unique_ptr<exec::Aggregate> createTypedCollectListAggregate(
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& resultType) {
  return std::make_unique<SimpleAggregateAdapter<TypedCollectListAggregate<T>>>(
      step, argTypes, resultType);
}

AggregateRegistrationResult registerCollectList(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("E")
          .returnType("array(E)")
          .intermediateType("array(E)")
          .argumentType("E")
          .build()};
  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_EQ(
            argTypes.size(), 1, "{} takes at most one argument", name);
        const bool isRawInput = exec::isRawInput(step);
        const TypePtr& inputType =
            isRawInput ? argTypes[0] : argTypes[0]->childAt(0);
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return createTypedCollectListAggregate<int8_t>(
                step, argTypes, resultType);
          case TypeKind::SMALLINT:
            return createTypedCollectListAggregate<int16_t>(
                step, argTypes, resultType);
          case TypeKind::INTEGER:
            return createTypedCollectListAggregate<int32_t>(
                step, argTypes, resultType);
          case TypeKind::BIGINT:
            return createTypedCollectListAggregate<int64_t>(
                step, argTypes, resultType);
          case TypeKind::REAL:
            return createTypedCollectListAggregate<float>(
                step, argTypes, resultType);
          case TypeKind::DOUBLE:
            return createTypedCollectListAggregate<double>(
                step, argTypes, resultType);
          default:
            break;
        }
        return std::make_unique<SimpleAggregateAdapter<CollectListAggregate>>(
            step, argTypes, resultType);
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace

void registerCollectListAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerCollectList(
      prefix + "collect_list", withCompanionFunctions, overwrite);
}
} // namespace bytedance::bolt::functions::aggregate::sparksql
