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

#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/PayloadRowWriter.h"

namespace bytedance::bolt::exec::radixsort {

inline void validatePayloadRowInput(
    const PayloadRowLayout& layout,
    const RowVector& input) {
  BOLT_CHECK_EQ(
      input.childrenSize(),
      layout.columns().size(),
      "Payload row input column count does not match layout");
  bool validChildren = true;
  bool validSizes = true;
  bool validTypes = true;
  for (uint32_t column = 0; column < layout.columns().size(); ++column) {
    const auto& child = input.childAt(column);
    validChildren &= child != nullptr;
    if (child != nullptr) {
      validSizes &= child->size() >= input.size();
      validTypes &= child->type()->equivalent(*layout.columns()[column].type);
    }
  }
  BOLT_CHECK(validChildren, "Payload row input child must not be null");
  BOLT_CHECK(validSizes, "Payload row input child is shorter than row vector");
  BOLT_CHECK(validTypes, "Payload row input type does not match layout");
}

inline bool canUseFlatScalarFastPath(
    const PayloadRowLayout& layout,
    const RowVector& input) {
  for (uint32_t column = 0; column < input.childrenSize(); ++column) {
    if (layout.columns()[column].complex) {
      return false;
    }
    if (input.childAt(column)->encoding() != VectorEncoding::Simple::FLAT) {
      return false;
    }
  }
  return true;
}

} // namespace bytedance::bolt::exec::radixsort
