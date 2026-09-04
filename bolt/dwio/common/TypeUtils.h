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

#include <optional>

#include "bolt/dwio/common/ColumnSelector.h"
#include "bolt/dwio/common/TypeWithId.h"
namespace bytedance::bolt::dwio::common::typeutils {

// Converts strings using Spark's boolean cast semantics.
std::optional<bool> sparkStringToBoolean(folly::StringPiece value);

// Build selected type based on input schema and selection
std::shared_ptr<const TypeWithId> buildSelectedType(
    const std::shared_ptr<const TypeWithId>& typeWithId,
    const std::function<bool(size_t)>& selector);

// Check type compatibility with full schema
void checkTypeCompatibility(
    const Type& from,
    const Type& to,
    bool recurse = false,
    const std::function<std::string()>& exceptionMessageCreator = nullptr);

// Check type compatibility based on selection
void checkTypeCompatibility(
    const Type& from,
    const ColumnSelector& selector,
    const std::function<std::string()>& exceptionMessageCreator = nullptr);

} // namespace bytedance::bolt::dwio::common::typeutils
