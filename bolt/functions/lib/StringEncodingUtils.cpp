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

#include "bolt/functions/lib/StringEncodingUtils.h"
#include "bolt/vector/VectorEncoding.h"
namespace bytedance::bolt::functions {

namespace {
/// Check if the input vector's buffers are single referenced
bool hasSingleReferencedBuffers(const FlatVector<StringView>& vector) {
  for (auto& buffer : vector.stringBuffers()) {
    if (!buffer->unique()) {
      return false;
    }
  }
  return true;
};
} // namespace

bool prepareFlatResultsVector(
    VectorPtr& result,
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    VectorPtr& argToReuse,
    const TypePtr& resultType) {
  BOLT_CHECK(resultType->isVarbinary() || resultType->isVarchar())

  bool reusedInput = false;
  if (!result && BaseVector::isVectorWritable(argToReuse) &&
      argToReuse->isFlatEncoding() &&
      hasSingleReferencedBuffers(*argToReuse->asFlatVector<StringView>())) {
    // Move input vector to result
    BOLT_CHECK(
        VectorEncoding::isFlat(argToReuse.get()->encoding()) &&
        argToReuse.get()->typeKind() == resultType->kind());

    result = std::move(argToReuse);
    reusedInput = true;
  } else {
    // This will allocate results if not allocated.
    BaseVector::ensureWritable(rows, resultType, context.pool(), result);
  }

  BOLT_CHECK(VectorEncoding::isFlat(result->encoding()));
  result->asFlatVector<StringView>()->clearStringViewStats();
  return reusedInput;
}

/// Return the string encoding of a vector, if not set UTF8 is returned
bool isAscii(BaseVector* vector, const SelectivityVector& rows) {
  if (auto simpleVector = vector->template as<SimpleVector<StringView>>()) {
    auto ascii = simpleVector->isAscii(rows);
    return ascii.has_value() && ascii.value();
  }
  BOLT_UNREACHABLE();
  return false;
};

} // namespace bytedance::bolt::functions
