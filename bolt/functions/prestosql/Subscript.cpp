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

#include "bolt/functions/lib/SubscriptUtil.h"
namespace bytedance::bolt::functions {

namespace {

/// Subscript operator for arrays and maps (to be mapped from usages like
/// array[0] and map['key']).
/// - does not allow negative indices for arrays
/// - does not allow out of bounds accesses for arrays (throws)
/// - index starts at 1 for arrays.
class SubscriptFunction : public SubscriptImpl<
                              /* allowNegativeIndices */ true,
                              /* nullOnNegativeIndices */ true,
                              /* allowOutOfBound */ true,
                              /* indexStartsAtOne */ true,
                              /* isElementAt */ false> {
 public:
  explicit SubscriptFunction(bool allowCaching) : SubscriptImpl(allowCaching) {}

  bool canPushdown() const override {
    return true;
  }
};

} // namespace

void registerSubscriptFunction(
    const std::string& name,
    bool enableCaching = true) {
  exec::registerStatefulVectorFunction(
      name,
      SubscriptFunction::signatures(),
      [enableCaching](
          const std::string&,
          const std::vector<exec::VectorFunctionArg>& inputArgs,
          const bolt::core::QueryConfig& config) {
        static const auto kSubscriptStateLess =
            std::make_shared<SubscriptFunction>(false);
        if (inputArgs[0].type->isArray()) {
          return kSubscriptStateLess;
        } else {
          return std::make_shared<SubscriptFunction>(
              enableCaching && config.isExpressionEvaluationCacheEnabled());
        }
      });
}

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_subscript,
    SubscriptFunction::signatures(),
    std::make_unique<SubscriptFunction>(false));

} // namespace bytedance::bolt::functions
