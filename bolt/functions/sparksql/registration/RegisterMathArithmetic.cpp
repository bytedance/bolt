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

#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/Arithmetic.h"
#include "bolt/functions/sparksql/Arithmetic.h"

namespace bytedance::bolt::functions::sparksql {

void registerMathArithmeticFunctions(const std::string& prefix) {
  // Operators.
  registerBinaryNumeric<PlusFunction>({prefix + "add"});
  registerBinaryNumeric<MinusFunction>({prefix + "subtract"});
  registerBinaryNumeric<MultiplyFunction>({prefix + "multiply"});
  registerFunction<DivideFunction, double, double, double>({prefix + "divide"});
  registerBinaryNumeric<RemainderFunction>({prefix + "remainder"});
  registerBinaryIntegralWithTReturn<IntegralDivideFunction, int64_t>(
      {prefix + "div"});
  registerUnaryNumeric<UnaryMinusFunction>({prefix + "unaryminus"});

  // Basic math functions.
  registerUnaryNumeric<AbsFunction>({prefix + "abs"});
  registerUnaryNumeric<NegateFunction>({prefix + "negative"});
  registerFunction<Log1pFunction, double, double>({prefix + "log1p"});
  registerFunction<ExpFunction, double, double>({prefix + "exp"});
  registerFunction<Expm1Function, double, double>({prefix + "expm1"});
  registerFunction<LnFunction, double, double>({prefix + "log"});
  registerFunction<LogFunction, double, double, double>({prefix + "log"});
  registerFunction<LnFunction, double, double>({prefix + "ln"});
  registerFunction<Log2Function, double, double>({prefix + "log2"});
  registerFunction<Log10Function, double, double>({prefix + "log10"});
  registerFunction<
      WidthBucketFunction,
      int64_t,
      double,
      double,
      double,
      int64_t>({prefix + "width_bucket"});

  // Modulo functions
  registerBinaryIntegral<PModIntFunction>({prefix + "pmod"});
  registerBinaryFloatingPoint<PModFloatFunction>({prefix + "pmod"});

  // Power function
  registerFunction<PowerFunction, double, double, double>({prefix + "power"});

  // Checked arithmetic functions
  registerBinaryNumeric<CheckedAddFunction>({prefix + "checked_add"});
  registerBinaryNumeric<CheckedSubtractFunction>({prefix + "checked_subtract"});
  registerBinaryNumeric<CheckedMultiplyFunction>({prefix + "checked_multiply"});
  registerBinaryNumeric<CheckedDivideFunction>({prefix + "checked_divide"});
  registerBinaryIntegralWithTReturn<CheckedIntegralDivideFunction, int64_t>(
      {prefix + "checked_div"});
}
} // namespace bytedance::bolt::functions::sparksql
