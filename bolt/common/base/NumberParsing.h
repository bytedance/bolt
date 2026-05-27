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

#include <cctype>
#include <charconv>
#include <cstdint>
#include <optional>
#include <string_view>
#include <system_error>

namespace bytedance::bolt {

inline std::optional<int64_t> parseInt64(std::string_view text) {
  int64_t number = 0;
  const auto* begin = text.data();
  const auto* end = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(begin, end, number);
  if (ec != std::errc() || ptr != end) {
    return std::nullopt;
  }
  return number;
}

inline std::optional<int64_t> extractNumberAfterPrefix(
    std::string_view text,
    std::string_view prefix) {
  const auto prefixPos = text.find(prefix);
  if (prefixPos == std::string_view::npos) {
    return std::nullopt;
  }

  const auto numStart = prefixPos + prefix.size();
  auto numEnd = numStart;
  while (numEnd < text.size() &&
         std::isdigit(static_cast<unsigned char>(text[numEnd]))) {
    ++numEnd;
  }
  if (numEnd == numStart) {
    return std::nullopt;
  }

  return parseInt64(text.substr(numStart, numEnd - numStart));
}

inline std::optional<int64_t> extractNumberBetween(
    std::string_view text,
    std::string_view prefix,
    std::string_view suffix) {
  const auto prefixPos = text.find(prefix);
  if (prefixPos == std::string_view::npos) {
    return std::nullopt;
  }

  const auto numStart = prefixPos + prefix.size();
  const auto numEnd = text.find(suffix, numStart);
  if (numEnd == std::string_view::npos || numEnd == numStart) {
    return std::nullopt;
  }

  return parseInt64(text.substr(numStart, numEnd - numStart));
}

} // namespace bytedance::bolt
