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

#include <string>
#include <string_view>
#include <vector>

#include "folly/Optional.h"
#include "folly/Range.h"
namespace bytedance::bolt::functions {

// The legacy flag mainly controls the behavior of the return results when
// encountering illegal characters If true, the already parsed portion of the
// results will be returned normally, and all unparsed parts will be null If
// false, all fields will return null
std::vector<folly::Optional<std::string>> jsonExtractTuple(
    folly::StringPiece json,
    std::vector<folly::Optional<folly::StringPiece>> paths,
    bool legacy = true,
    const bool useSonicLibrary = true);

// Currently, bolt does not throw exception,
folly::Optional<std::string_view> jsonExtractScalar(
    folly::StringPiece json,
    folly::StringPiece path,
    const bool throwExceptionWhenEncounterBadJson = false,
    const bool useDOMParserInGetJsonObject = false,
    const bool getJsonObjectEscapeEmoji = true,
    const bool parsePath = true,
    const bool useSonicJson = false);

folly::Optional<std::string_view> jsonExtractScalar(
    const std::string& json,
    const std::string& path,
    const bool throwExceptionWhenEncounterBadJson = false,
    const bool useDOMParserInGetJsonObject = false,
    const bool getJsonObjectEscapeEmoji = true,
    const bool useSonicJson = true);

// For json size function
folly::Optional<size_t> jsonExtractSize(
    const std::string& json,
    const std::string& path);

folly::Optional<size_t> jsonExtractSize(
    folly::StringPiece json,
    folly::StringPiece path);

// For json_array_length()
folly::Optional<size_t> jsonArrayLength(folly::StringPiece json);

} // namespace bytedance::bolt::functions
