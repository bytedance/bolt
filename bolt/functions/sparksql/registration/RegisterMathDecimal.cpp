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

namespace bytedance::bolt::functions {

void registerMathDecimalFunctionsInternal(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_abs, prefix + "abs");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_negate, prefix + "negative");
}

namespace sparksql {
void registerMathDecimalFunctions(const std::string& prefix) {
  registerMathDecimalFunctionsInternal(prefix);

  // Decimal precision-aware operations
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_add_deny_precision_loss, prefix + "add_deny_precision_loss");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_sub_deny_precision_loss,
      prefix + "subtract_deny_precision_loss");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_mul_deny_precision_loss,
      prefix + "multiply_deny_precision_loss");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_div_deny_precision_loss,
      prefix + "divide_deny_precision_loss");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_add, prefix + "add");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_sub, prefix + "subtract");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_mul, prefix + "multiply");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_div, prefix + "divide");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_integral_div, prefix + "div");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_checked_integral_div, prefix + "checked_div");

  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_ceil, prefix + "ceil");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_floor, prefix + "floor");
}
} // namespace sparksql
} // namespace bytedance::bolt::functions
