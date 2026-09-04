/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <folly/Range.h>
#include <unicode/unistr.h>

namespace bytedance::bolt::functions::stringCore::spark {

/// Rewrites Greek capital sigmas so ICU lowercasing reproduces ByteOpenJDK
/// 17.0.9+9 root-locale final-sigma semantics. sigmaOffsets contains the UTF-16
/// offset of every U+03A3 in input, in ascending order.
void adjustJavaSigmaInPlace(
    icu::UnicodeString& input,
    folly::Range<const int32_t*> sigmaOffsets);

} // namespace bytedance::bolt::functions::stringCore::spark
