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

#include "bolt/functions/lib/SubscriptUtil.h"

namespace bytedance::bolt::functions::flinksql {

namespace {

/// element_at(array, index) for Flink SQL semantics:
///
/// - index is 1-based.
/// - If index is a compile-time constant and index < 1, throws an exception.
/// - If index is a runtime (non-constant) value and index < 1 or out of
///   bounds, returns NULL silently.
/// - Negative indices are NOT allowed (unlike Presto/Spark).
///
/// Template parameters:
///   allowNegativeIndices = false             (not needed;
///   nullOnNonConstantInvalidIndex
///                                             catches all index < 1 before
///                                             getIndex)
///   nullOnNegativeIndices = false            (not used)
///   allowOutOfBound = true                   (OOB → NULL)
///   indexStartsAtOne = true                  (1-based)
///   isElementAt = true
///   nullOnNonConstantInvalidIndex = true     (constant < 1 → throw;
///   non-constant < 1 → NULL)
class FlinkElementAtFunction : public SubscriptImpl<
                                   /* allowNegativeIndices */ false,
                                   /* nullOnNegativeIndices */ false,
                                   /* allowOutOfBound */ true,
                                   /* indexStartsAtOne */ true,
                                   /* isElementAt */ true,
                                   /* nullOnNonConstantInvalidIndex */ true> {
 public:
  FlinkElementAtFunction(bool allowCaching, bool constantIndexInvalidThrows)
      : SubscriptImpl(allowCaching, constantIndexInvalidThrows) {}
};

} // namespace

void registerFlinkElementAtFunction(const std::string& name) {
  exec::registerStatefulVectorFunction(
      name,
      FlinkElementAtFunction::signatures(),
      [](const std::string&,
         const std::vector<exec::VectorFunctionArg>& inputArgs,
         const bolt::core::QueryConfig& config) {
        const bool constantIndexInvalidThrows =
            inputArgs.size() > 1 && inputArgs[1].constantValue != nullptr;
        if (inputArgs[0].type->isArray()) {
          return std::make_shared<FlinkElementAtFunction>(
              false, constantIndexInvalidThrows);
        } else {
          return std::make_shared<FlinkElementAtFunction>(
              config.isExpressionEvaluationCacheEnabled(),
              constantIndexInvalidThrows);
        }
      });
}

} // namespace bytedance::bolt::functions::flinksql
