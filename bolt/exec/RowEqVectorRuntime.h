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

#include <cstdint>

#include "bolt/vector/TypeAliases.h"

namespace bytedance::bolt {

class DecodedVector;

namespace exec {

struct RowEqVectorRuntime {
  const void* values;
  const vector_size_t* indices;
  const uint64_t* nulls;
  const DecodedVector* decodedVector;
  vector_size_t constantIndex;
  bool isIdentityMapping;
  bool isConstantMapping;
  bool nullsUseTopLevelIndex;
};

} // namespace exec
} // namespace bytedance::bolt
