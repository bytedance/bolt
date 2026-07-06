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

#include <string_view>

namespace bytedance::bolt::functions::sparksql {

/// Helper implementation for Spark's StringSplitSQL semantics.
///
/// This matches Spark's UTF8String.splitSQL behavior:
///  * Byte-based delimiter search.
///  * Keeps empty tokens including trailing empty tokens.
///  * When delimiter is empty, returns a single-element array [input].
///
/// Caller is responsible for handling null inputs.
template <typename Consumer>
inline void stringSplitSQLImpl(
    std::string_view input,
    std::string_view delimiter,
    Consumer&& emit) {
  if (delimiter.empty()) {
    emit(input);
    return;
  }

  std::size_t pos = 0;
  const std::size_t dlen = delimiter.size();

  while (true) {
    auto found = input.find(delimiter, pos);
    if (found == std::string_view::npos) {
      // Emit the last segment (may be empty when input ends with delimiter).
      emit(input.substr(pos));
      break;
    }

    emit(input.substr(pos, found - pos));
    pos = found + dlen;
  }
}

} // namespace bytedance::bolt::functions::sparksql
