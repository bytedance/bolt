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

namespace bytedance::bolt::functions::sparksql {

namespace {

// get(array, index) -> array[index]
//
// - allows negative indices for arrays (returns NULL if index < 0).
// - allows out of bounds accesses for arrays (returns NULL if out of
//    bounds).
// - index starts at 0 for arrays.
class ArrayGetFunction : public SubscriptImpl<
                             /* allowNegativeIndices */ true,
                             /* nullOnNegativeIndices */ true,
                             /* allowOutOfBound */ true,
                             /* indexStartsAtOne */ false,
                             /* isElementAt */ false> {
 public:
  explicit ArrayGetFunction() : SubscriptImpl(false) {}

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
    for (const auto& indexType : {"integer", "bigint"}) {
      signatures.push_back(exec::FunctionSignatureBuilder()
                               .typeVariable("T")
                               .returnType("T")
                               .argumentType("array(T)")
                               .argumentType(indexType)
                               .build());
    }
    return signatures;
  }
};
} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_array_get,
    ArrayGetFunction::signatures(),
    std::make_unique<ArrayGetFunction>());

} // namespace bytedance::bolt::functions::sparksql
