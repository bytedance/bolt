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

#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/sparksql/Bitwise.h"
namespace bytedance::bolt::functions::sparksql {

void registerBitwiseFunctions(const std::string& prefix) {
  registerBinaryIntegral<BitwiseXorFunction>({prefix + "bitwise_xor"});
  registerUnaryIntegral<BitwiseNotFunction>({prefix + "bitwise_not"});
  registerBinaryIntegral<BitwiseAndFunction>({prefix + "bitwise_and"});
  registerBinaryIntegral<BitwiseOrFunction>({prefix + "bitwise_or"});

  registerFunction<BitCountFunction, int32_t, bool>({prefix + "bit_count"});
  registerFunction<BitCountFunction, int32_t, int8_t>({prefix + "bit_count"});
  registerFunction<BitCountFunction, int32_t, int16_t>({prefix + "bit_count"});
  registerFunction<BitCountFunction, int32_t, int32_t>({prefix + "bit_count"});
  registerFunction<BitCountFunction, int32_t, int64_t>({prefix + "bit_count"});

  registerFunction<BitDaysCountFunction, int32_t, Array<int64_t>>(
      {prefix + "bit_days_count"});

  registerFunction<BitDaysCountFunction, int32_t, Array<int64_t>, int32_t>(
      {prefix + "bit_days_count"});

  registerFunction<BitGetFunction, int8_t, int8_t, int32_t>(
      {prefix + "bit_get"});
  registerFunction<BitGetFunction, int8_t, int16_t, int32_t>(
      {prefix + "bit_get"});
  registerFunction<BitGetFunction, int8_t, int32_t, int32_t>(
      {prefix + "bit_get"});
  registerFunction<BitGetFunction, int8_t, int64_t, int32_t>(
      {prefix + "bit_get"});

  registerFunction<ShiftLeftFunction, int32_t, int32_t, int32_t>(
      {prefix + "shiftleft"});
  registerFunction<ShiftLeftFunction, int64_t, int64_t, int32_t>(
      {prefix + "shiftleft"});

  registerFunction<ShiftRightFunction, int32_t, int32_t, int32_t>(
      {prefix + "shiftright"});
  registerFunction<ShiftRightFunction, int64_t, int64_t, int32_t>(
      {prefix + "shiftright"});

  registerFunction<ShiftRightUnsignedFunction, int32_t, int32_t, int32_t>(
      {prefix + "shiftrightunsigned"});
  registerFunction<ShiftRightUnsignedFunction, int64_t, int64_t, int32_t>(
      {prefix + "shiftrightunsigned"});
}

} // namespace bytedance::bolt::functions::sparksql
