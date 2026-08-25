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

#include "bolt/common/base/SparkCompatibility.h"

#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/Bitwise.h"
namespace bytedance::bolt::functions {
namespace {
template <template <class> class T>
void registerBitwiseBinaryIntegral(const std::vector<std::string>& aliases) {
  registerFunction<T, int64_t, int8_t, int8_t>(aliases);
  registerFunction<T, int64_t, int16_t, int16_t>(aliases);
  registerFunction<T, int64_t, int32_t, int32_t>(aliases);
  registerFunction<T, int64_t, int64_t, int64_t>(aliases);
}

template <template <class> class T>
void registerBitwiseUnaryIntegral(const std::vector<std::string>& aliases) {
  registerFunction<T, int64_t, int8_t>(aliases);
  registerFunction<T, int64_t, int16_t>(aliases);
  registerFunction<T, int64_t, int32_t>(aliases);
  registerFunction<T, int64_t, int64_t>(aliases);
}
} // namespace

void registerBitwiseFunctions(const std::string& prefix) {
  registerBitwiseBinaryIntegral<BitwiseAndFunction>({prefix + "bitwise_and"});
  registerBitwiseUnaryIntegral<BitwiseNotFunction>({prefix + "bitwise_not"});
  registerBitwiseBinaryIntegral<BitwiseOrFunction>({prefix + "bitwise_or"});
  registerBitwiseBinaryIntegral<BitwiseXorFunction>({prefix + "bitwise_xor"});
  if constexpr (::bytedance::bolt::kSparkCompatible) {
    registerBitwiseBinaryIntegral<BitCountFunction>({prefix + "bit_count"});
  } else {
    // built-in bit_count(bigint, bigint) -> bigint
    registerFunction<BitCountFunction, int64_t, int64_t, int64_t>(
        {prefix + "bit_count"});

    // hive.udf.bit_count(bigint) -> integer
    registerFunction<BitCountFunction, int32_t, int64_t>(
        {prefix + "bit_count"});
  }

  registerFunction<BitDaysCountFunction, int32_t, Array<int64_t>>(
      {prefix + "bit_days_count"});
  registerFunction<BitDaysCountFunction, int32_t, Array<int64_t>, int32_t>(
      {prefix + "bit_days_count"});

  registerFunction<
      BitwiseArithmeticShiftRightFunction,
      int64_t,
      int64_t,
      int64_t>({prefix + "bitwise_arithmetic_shift_right"});
  registerBitwiseBinaryIntegral<BitwiseLeftShiftFunction>(
      {prefix + "bitwise_left_shift"});
  registerBitwiseBinaryIntegral<BitwiseRightShiftFunction>(
      {prefix + "bitwise_right_shift"});
  registerBitwiseBinaryIntegral<BitwiseRightShiftArithmeticFunction>(
      {prefix + "bitwise_right_shift_arithmetic"});
  registerFunction<
      BitwiseLogicalShiftRightFunction,
      int64_t,
      int64_t,
      int64_t,
      int64_t>({prefix + "bitwise_logical_shift_right"});
  registerFunction<
      BitwiseShiftLeftFunction,
      int64_t,
      int64_t,
      int64_t,
      int64_t>({prefix + "bitwise_shift_left"});
}

} // namespace bytedance::bolt::functions
