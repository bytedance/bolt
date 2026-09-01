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

#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::utf8 {

/// Replaces malformed UTF-8 in top-level VARCHAR children using OpenJDK's
/// malformed-input grouping. Top-level CONSTANT vectors, VARBINARY and nested
/// VARCHAR values are left unchanged. Returns 'input' when no replacement is
/// needed and never mutates the input vector.
RowVectorPtr replaceInvalidUtf8InTopLevelVarchars(
    const RowVectorPtr& input,
    memory::MemoryPool* pool);

} // namespace bytedance::bolt::utf8
