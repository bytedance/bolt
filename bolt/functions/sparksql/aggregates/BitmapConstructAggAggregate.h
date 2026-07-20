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
#include <string>

#include "bolt/exec/AggregateUtil.h"

namespace bytedance::bolt::functions::aggregate::sparksql {

// Matching Spark's BitmapExpressionUtils.NUM_BYTES (4 KiB, 32768 bits).
inline constexpr int32_t kBitmapNumBytes = 4096;
inline constexpr int32_t kBitmapNumBits = kBitmapNumBytes * 8;

exec::AggregateRegistrationResult registerBitmapConstructAggAggregate(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite);

} // namespace bytedance::bolt::functions::aggregate::sparksql
