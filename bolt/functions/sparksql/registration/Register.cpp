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

#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/expression/SimpleFunctionRegistry.h"
#include "bolt/expression/SpecialFormRegistry.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
namespace bytedance::bolt::functions::sparksql {

extern void registerArrayFunctions(const std::string& prefix);
extern void registerBinaryFunctions(const std::string& prefix);
extern void registerBitwiseFunctions(const std::string& prefix);
extern void registerCompareFunctions(const std::string& prefix);
extern void registerDatetimeFunctions(const std::string& prefix);
extern void registerJsonFunctions(const std::string& prefix);
extern void registerMapFunctions(const std::string& prefix);
extern void registerMathFunctions(const std::string& prefix);
extern void registerMiscFunctions(const std::string& prefix);
extern void registerRegexpFunctions(const std::string& prefix);
extern void registerSpecialFormGeneralFunctions(const std::string& prefix);
extern void registerStringFunctions(const std::string& prefix);
extern void registerUrlFunctions(const std::string& prefix);
extern void registerVariantFunctions(const std::string& prefix);

void registerFunctions(const std::string& prefix) {
  registerTimestampWithTimeZoneType();
  registerArrayFunctions(prefix);
  registerBinaryFunctions(prefix);
  registerBitwiseFunctions(prefix);
  registerCompareFunctions(prefix);
  registerDatetimeFunctions(prefix);
  registerJsonFunctions(prefix);
  registerMapFunctions(prefix);
  registerMathFunctions(prefix);
  registerMiscFunctions(prefix);
  registerRegexpFunctions(prefix);
  registerSpecialFormGeneralFunctions(prefix);
  registerStringFunctions(prefix);
  registerUrlFunctions(prefix);
  registerVariantFunctions(prefix);
}

} // namespace bytedance::bolt::functions::sparksql
