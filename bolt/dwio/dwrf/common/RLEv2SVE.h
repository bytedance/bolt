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

#pragma once

#include <cstdint>

namespace bytedance::bolt::dwrf::detail {

// These helpers require SVE. Call only after process::hasSve() succeeds.
// [offset, end) uses row positions; null bits set to one denote valid rows.
uint64_t fillRepeatedSve(
    int64_t* data,
    uint64_t offset,
    uint64_t end,
    const uint64_t* nulls,
    int64_t value);

void zigzagDecodeSve(
    int64_t* data,
    uint64_t offset,
    uint64_t end,
    const uint64_t* nulls);

// Returns the number of non-null values written and updates the carry.
uint64_t fixedDeltaSve(
    int64_t* data,
    uint64_t offset,
    uint64_t end,
    const uint64_t* nulls,
    int64_t delta,
    int64_t& previous);

void variableDeltaSve(
    int64_t* data,
    uint64_t offset,
    uint64_t end,
    const uint64_t* nulls,
    bool negative,
    int64_t& previous);

} // namespace bytedance::bolt::dwrf::detail
