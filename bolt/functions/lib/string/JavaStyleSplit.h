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

#include <unicode/localpointer.h>
#include <unicode/regex.h>
#include <unicode/unistr.h>
#include <cstddef>
#include <cstring>
#include "bolt/expression/VectorWriters.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::functions {
namespace javaStyle {
template <bool isSpark34Plus = false>
static std::vector<std::string_view> javaStyleSplitFastEmptyPattern(
    std::string_view input,
    std::string_view /*unusend*/,
    int32_t limit) {
  std::vector<std::string_view> result;
  bool limited = limit > 0;
  int32_t splitLimit = isSpark34Plus ? limit + 1 : limit;
  size_t i = 0;
  while (i < input.size()) {
    if (!limited || result.size() < static_cast<size_t>(splitLimit) - 1) {
      unsigned char lead = input[i];

      size_t charLen = 1;
      if ((lead >> 5) == 0x6) { // 110xxxxx, 2 Bytes
        charLen = 2;
      } else if ((lead >> 4) == 0xE) { // 1110xxxx, 3 Bytes
        charLen = 3;
      } else if ((lead >> 3) == 0x1E) { // 11110xxx, 4 Bytes
        charLen = 4;
      }

      if (i + charLen <= input.size()) {
        result.emplace_back(&input[i], charLen);
      } else {
        BOLT_FAIL("Invalid UTF-8 sequence at position: {}", i);
        break;
      }
      i += charLen;
    } else {
      result.emplace_back(input.data() + i, input.size() - i);
      i = input.size();
      break;
    }
  }
  if (i == 0) {
    return {input};
  }
  if (!limited || result.size() < static_cast<size_t>(splitLimit)) {
    result.emplace_back(input.data() + i, input.size() - i);
  }
  if (limit == 0 || isSpark34Plus) {
    while (!result.empty() && result.back().empty()) {
      result.pop_back();
    }
  }
  if (isSpark34Plus) {
    if (limited) {
      // for spark 3.4+, split empty string will divide each character, so
      // split("hello", "", 3) => ["h","e","l"] instead of ["h","e","llo"]
      BOLT_CHECK(result.size() <= static_cast<size_t>(splitLimit));
      if (result.size() == splitLimit) {
        // remove the last string
        result.pop_back();
      }
    }
  }

  return result;
}

template <bool isSpark34Plus = false>
static void javaStyleSplitFastEmptyPatternWriter(
    std::string_view input,
    int32_t limit,
    exec::ArrayWriter<Varchar>& arrayWriter) {
  if constexpr (isSpark34Plus) {
    auto result = javaStyleSplitFastEmptyPattern<true>(input, "", limit);
    for (const auto& item : result) {
      arrayWriter.add_item().setNoCopy(StringView(item));
    }
    return;
  }

  // Before Spark 3.4, an empty regex is handled by Java String.split and
  // therefore works on UTF-16 code units. A supplementary Unicode character
  // becomes two isolated surrogate strings. Java's UTF-8 encoder replaces
  // each isolated surrogate with '?'.
  if (input.empty()) {
    arrayWriter.add_item().setNoCopyUnsafe(StringView(input));
    return;
  }

  const bool limited = limit > 0;
  if (limited && limit == 1) {
    arrayWriter.add_item().setNoCopyUnsafe(StringView(input));
    return;
  }

  size_t offset = 0;
  size_t resultSize = 0;
  while (offset < input.size()) {
    if (limited && resultSize >= static_cast<size_t>(limit) - 1) {
      arrayWriter.add_item().setNoCopyUnsafe(
          StringView(input.data() + offset, input.size() - offset));
      return;
    }

    const unsigned char lead = input[offset];
    size_t charLength = 1;
    if ((lead >> 5) == 0x6) { // 110xxxxx, 2 bytes
      charLength = 2;
    } else if ((lead >> 4) == 0xE) { // 1110xxxx, 3 bytes
      charLength = 3;
    } else if ((lead >> 3) == 0x1E) { // 11110xxx, 4 bytes
      charLength = 4;
    }

    BOLT_CHECK_LE(
        offset + charLength,
        input.size(),
        "Invalid UTF-8 sequence at position: {}",
        offset);

    if (charLength != 4) {
      arrayWriter.add_item().setNoCopyUnsafe(
          StringView(input.data() + offset, charLength));
      offset += charLength;
      ++resultSize;
      continue;
    }

    arrayWriter.add_item().copy_from(StringView("?"));
    ++resultSize;

    if (limited && resultSize >= static_cast<size_t>(limit) - 1) {
      // The final field starts with an isolated low surrogate when the limit
      // boundary falls between the pair. Encode it as '?' and preserve the
      // remaining valid UTF-8 suffix.
      std::string remainder;
      remainder.reserve(1 + input.size() - offset - charLength);
      remainder.push_back('?');
      remainder.append(
          input.data() + offset + charLength,
          input.size() - offset - charLength);
      arrayWriter.add_item().copy_from(StringView(remainder));
      return;
    }

    arrayWriter.add_item().copy_from(StringView("?"));
    ++resultSize;
    offset += charLength;
  }

  if (!limited || resultSize < static_cast<size_t>(limit)) {
    arrayWriter.add_item().setNoCopyUnsafe(StringView(""));
  }
}

static std::vector<std::string_view> javaStyleSplitFast(
    std::string_view input,
    std::string_view pattern,
    int32_t limit) {
  size_t off = 0;
  size_t next = 0;
  bool limited = limit > 0;
  std::vector<std::string_view> result;
  auto psize = pattern.size();
  while ((next = input.find(pattern, off)) != std::string::npos) {
    if (!limited || result.size() < static_cast<size_t>(limit) - 1) {
      result.emplace_back(input.data() + off, next - off);
      off = next + psize;
    } else {
      result.emplace_back(input.data() + off, input.size() - off);
      off = input.size();
      break;
    }
  }
  if (off == 0) {
    return {input};
  }

  if (!limited || result.size() < static_cast<size_t>(limit)) {
    result.emplace_back(input.data() + off, input.size() - off);
  }

  if (UNLIKELY(limit == 0)) {
    while (!result.empty() && result.back().empty()) {
      result.pop_back();
    }
  }

  return result;
}

// only optimize the common case, others keep the same as they are depended on
// by some interfaces, like str_to_map.

static void javaStyleSplitFastWriter(
    std::string_view input,
    std::string_view pattern,
    int32_t limit,
    exec::ArrayWriter<Varchar>& arrayWriter) {
  size_t off = 0;
  size_t next = 0;
  bool limited = limit > 0;
  size_t resNum = 0;
  auto psize = pattern.size();
  while ((next = input.find(pattern, off)) != std::string::npos) {
    if (!limited || resNum < static_cast<size_t>(limit) - 1) {
      arrayWriter.add_item().setNoCopyUnsafe(
          StringView(input.data() + off, next - off));
      off = next + psize;
      resNum++;
    } else {
      arrayWriter.add_item().setNoCopyUnsafe(
          StringView(input.data() + off, input.size() - off));
      off = input.size();
      resNum++;
      break;
    }
  }
  if (off == 0) {
    arrayWriter.add_item().setNoCopyUnsafe(StringView(input));
    return;
  }

  if (!limited || resNum < static_cast<size_t>(limit)) {
    arrayWriter.add_item().setNoCopyUnsafe(
        StringView(input.data() + off, input.size() - off));
  }
  // limit is never to be 0, so ignore it.
  return;
}

static std::vector<std::string_view> javaStyleSplitRegex(
    std::string_view input,
    const icu::LocalPointer<icu::RegexPattern>& pattern,
    int32_t limit) {
  UErrorCode status = U_ZERO_ERROR;
  icu::UnicodeString UInput = icu::UnicodeString::fromUTF8(
      icu::StringPiece(input.data(), input.size()));
  std::vector<std::string_view> results;
  icu::LocalPointer<icu::RegexMatcher> matcher(
      pattern->matcher(UInput, status));
  size_t index = 0;
  bool matchLimited = limit > 0;
  while (matcher->find()) {
    size_t matchStart = matcher->start(status);
    BOLT_CHECK(
        U_SUCCESS(status),
        "Error split by regex pattern: {}",
        u_errorName(status));
    size_t matchEnd = matcher->end(status);
    BOLT_CHECK(
        U_SUCCESS(status),
        "Error split by regex pattern: {}",
        u_errorName(status));
    auto origIndex = stringCore::char16IndexToByteIndex(
        UInput.getBuffer(), input.size(), index);
    auto origMatchStart = stringCore::char16IndexToByteIndex(
        UInput.getBuffer(), input.size(), matchStart);
    auto origMatchEnd = stringCore::char16IndexToByteIndex(
        UInput.getBuffer(), input.size(), matchEnd);
    if (!matchLimited || results.size() < limit - 1) {
      if (index == 0 && index == matchStart && matchStart == matchEnd) {
        // no empty leading substring included for zero-width match
        // at the beginning of the input char sequence.
        continue;
      }
      results.emplace_back(
          input.data() + origIndex, origMatchStart - origIndex);
      index = matchEnd;
    } else if (results.size() == limit - 1) { // last one
      results.emplace_back(input.data() + origIndex, input.size() - origIndex);
      index = matchEnd;
    }
  }

  // If no match was found, return this
  if (index == 0) {
    results.clear();
    results.emplace_back(input);
  } else {
    // Add remaining segment
    auto origIndex = stringCore::char16IndexToByteIndex(
        UInput.getBuffer(), input.size(), index);
    if (!matchLimited || results.size() < limit)
      results.emplace_back(input.data() + origIndex, input.size() - origIndex);

    // Construct result
    if (limit == 0) {
      while (!results.empty() && results.back().empty()) {
        results.pop_back();
      }
    }
  }
  return results;
}
static std::vector<std::string_view> javaStyleSplitRegex(
    std::string_view input,
    std::string_view pattern,
    int32_t limit) {
  UErrorCode status = U_ZERO_ERROR;
  icu::LocalPointer<icu::RegexPattern> regex(icu::RegexPattern::compile(
      icu::UnicodeString::fromUTF8(
          icu::StringPiece(pattern.data(), pattern.size())),
      0,
      status));
  BOLT_CHECK(
      U_SUCCESS(status),
      "Error compiling regex pattern: {}",
      u_errorName(status));
  return javaStyleSplitRegex(input, regex, limit);
}

template <bool isSpark34Plus = false>
static std::vector<std::string_view> javaStyleSplit(
    std::string_view input,
    std::string_view pattern,
    int32_t limit) {
  BOLT_CHECK(
      !pattern.empty(),
      "Empty pattern must be handled by javaStyleSplitFastEmptyPattern");
  std::string newPattern{pattern};
  if (!stringCore::checkRegexPattern(newPattern)) {
    return javaStyleSplitFast(input, newPattern, limit);
  } else {
    return javaStyleSplitRegex(input, pattern, limit);
  }
}

} // namespace javaStyle
} // namespace bytedance::bolt::functions
