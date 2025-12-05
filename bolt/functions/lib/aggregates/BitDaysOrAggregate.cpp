/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include "bolt/functions/lib/aggregates/BitDaysOrAggregate.h"

#include <expression/ComplexWriterTypes.h>
#include "bolt/exec/SimpleAggregateAdapter.h"
namespace bytedance::bolt::functions::aggregate {

namespace {

/**
 * 业务背景：
 *
 *
 * 一个bigint，共64位.
 * 符号位始终为0.
 * 占位符始终为1:
 * 1、方便进行边界判断，当高63位为1，说明达到阈值，该bigint已经存满，需要向数组中新增bigint进行存储
 * 2、当用户活跃天数为0时，如果没有符号位，bigint的值一直为0，无法判断不活跃天数对应的具体天数顺序，因此添加符号位
 * 综上，总共62位可用。
 *
 *
 * bigint二进制表示                                               十进制
 * 0000 0000... 0000 0010    进组时间为1天，这1天都没活跃             (2)
 * 0100 0000... 0000 0000    进组时间为62天，这62天每天都没活跃
 * (4611686018427387904) 0000 0000... 0000 0111 进组时间为2天，这2天每天都活跃
 * (7) 0100 0000... 0000 0011    进组时间为62天，这62天只在最后2天活跃
 * (4611686018427387907) 0111 0000... 0000 0000
 * 进组时间为62天，这62天只在前2天活跃      (8070450532247928832)
 *
 *
 * 第63天进组，需要向bigint数组中新增一个bigint，在新增bigint中进行存储
 * 0000 0000... 0000 0011，0111 1111... 1111 1110
 * 进组63天，前62天活跃，第63天不活跃 0000 0000... 0000 0011，0111 1111... 1111
 * 1111     进组63天，前62天活跃，第63天也活跃
 *
 *
 */

// bit_days_or(array<bigint>) -> array<bigint> -> array<bigint>
class BitDaysOrAggregate {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<Array<int64_t>>;

  // Type of intermediate result vector.
  using IntermediateType = Array<int64_t>;

  // Type of output vector.
  using OutputType = Array<int64_t>;

  inline static uint64_t highestOneBit(uint64_t value) {
    if (UNLIKELY(value == 0)) {
      return 0;
    }
    return 1LL << (63 - __builtin_clzll(value));
  }

  static void arrayBitOr(
      std::vector<int64_t, StlAllocator<int64_t>>& result,
      const exec::arg_type<Array<int64_t>>& input) {
    if (result.empty()) {
      result.reserve(input.size());
      for (auto element : input) {
        result.push_back(element.value());
      }
    } else {
      int resultLength = result.size() - 1;
      int inputLength = input.size() - 1;
      int maxLength = std::max(resultLength, inputLength);
      result.resize(maxLength + 1);

      while (resultLength >= 0 && inputLength >= 0) {
        int64_t resultValue = result[resultLength];
        int64_t inputValue = input[inputLength].value();
        int64_t res = resultValue < inputValue
            ? (~highestOneBit(resultValue) & resultValue) | inputValue
            : (~highestOneBit(inputValue) & inputValue) | resultValue;
        result[maxLength--] = res;
        resultLength--;
        inputLength--;
      }

      while (resultLength >= 0) {
        result[maxLength--] = result[resultLength--];
      }

      while (inputLength >= 0) {
        result[maxLength--] = input[inputLength--].value();
      }
    }
  }

  struct AccumulatorType {
    std::vector<int64_t, StlAllocator<int64_t>> result;

    AccumulatorType() = delete;

    explicit AccumulatorType(HashStringAllocator* allocator)
        : result(StlAllocator<int64_t>(allocator)) {}

    static constexpr bool is_fixed_size_ = false;

    static void copyResultToArrayWriter(
        const std::vector<int64_t, StlAllocator<int64_t>>& result,
        exec::ArrayWriter<int64_t>& writer) {
      writer.resetLength();
      auto size = result.size();
      if (size == 0) {
        return;
      }
      writer.resize(size);
      for (auto i = 0; i < size; ++i) {
        writer[i] = result[i];
      }
    }

    bool addInput(
        HashStringAllocator* /*allocator*/,
        exec::arg_type<Array<int64_t>> data) {
      if (data.empty()) {
        return true;
      }
      arrayBitOr(result, data);
      return true;
    }

    bool combine(
        HashStringAllocator* /* allocator */,
        exec::arg_type<IntermediateType> other) {
      if (other.empty()) {
        return false;
      }
      arrayBitOr(result, other);
      return true;
    }

    bool writeFinalResult(exec::out_type<OutputType>& out) {
      if (result.empty()) {
        return false;
      }
      copyResultToArrayWriter(result, out);
      return true;
    }

    bool writeIntermediateResult(exec::out_type<IntermediateType>& out) {
      if (result.empty()) {
        return false;
      }
      copyResultToArrayWriter(result, out);
      return true;
    }
  };
};

} // namespace

exec::AggregateRegistrationResult registerBitDaysOrAggregate(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("array(bigint)")
          .intermediateType("array(bigint)")
          .argumentType("array(bigint)")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_EQ(
            argTypes.size(), 1, "{} takes at most one argument", name);
        return std::make_unique<
            exec::SimpleAggregateAdapter<BitDaysOrAggregate>>(resultType);
      },
      /*registerCompanionFunctions*/ withCompanionFunctions,
      overwrite);
}

} // namespace bytedance::bolt::functions::aggregate
