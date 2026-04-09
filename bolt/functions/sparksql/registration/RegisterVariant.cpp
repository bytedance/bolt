/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/sparksql/VariantFunctions.h"

namespace bytedance::bolt::functions::sparksql {

void registerVariantFunctions(const std::string& prefix) {
  registerFunction<ParseJsonFunction, Variant, Varchar>(
      {prefix + "parse_json"});
  registerFunction<VariantGetFunction, Varchar, Variant, Varchar>(
      {prefix + "variant_get"});
}

} // namespace bytedance::bolt::functions::sparksql
