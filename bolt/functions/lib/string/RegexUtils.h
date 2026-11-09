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

#include <re2/re2.h>
#include <string>
#include <string_view>
#include <utility>
#include "bolt/type/StringView.h"
#include "folly/CPortability.h"
namespace bytedance::bolt::functions::regex {

static FOLLY_ALWAYS_INLINE std::string transDanglingRightBrackets(
    const char* begin,
    size_t size) {
  std::string result;
  const char* cur = begin;
  const char* end = cur + size;
  int curly_bracket_count = 0;
  while (cur != end) {
    bool literal = false;
    if (*cur == '\\') {
      while (cur != end && *cur == '\\') {
        cur++;
        literal = !literal;
        result.push_back('\\');
      }
    }
    if (cur != end) {
      if (!literal) {
        switch (*cur) {
          case '{':
            curly_bracket_count++;
            break;
          case '}':
            if (curly_bracket_count == 0) {
              curly_bracket_count++;
              result.push_back('\\');
            }
            curly_bracket_count--;
            break;
          default:
            break;
        }
      }
      result.push_back(*cur);
      cur++;
    }
  }
  return result;
}

static inline bool
matchSequence(const char* cur, const char* end, const std::string& seq) {
  size_t len = seq.length();
  if (cur + len > end)
    return false;
  return std::memcmp(cur, seq.data(), len) == 0;
}

/**
 * @brief Transforms Java-style regex character classes to ICU-compatible regex
 * character classes.
 *
 * This function processes a regex pattern and replaces Java-specific shorthand
 * character classes
 * (\w, \W, \d, \D, \s, \S) with their explicit equivalents to ensure consistent
 * behavior in ICU. Additionally, it handles special combinations like [\w\W],
 * [\d\D], and [\s\S], replacing them with ICU-compatible patterns to match any
 * character, similar to Java's [\w\W].
 *
 * @param begin Pointer to the beginning of the regex pattern string.
 * @param size Length of the regex pattern string.
 * @return A transformed regex pattern string compatible with ICU.
 */

static inline std::string transCharacterClass(const char* begin, size_t size) {
  // Mapping of single shorthand characters to their explicit ICU equivalents.
  std::unordered_map<char, std::string> singleShorthand{
      {'w', "a-zA-Z0-9_"}, // \w → [a-zA-Z0-9_]
      {'d', "0-9"}, // \d → [0-9]
      {'s', " \t\n\x0B\f\r"}, // \s → [ \t\n\x0B\f\r]
      {'W', "^a-zA-Z0-9_"}, // \W → [^a-zA-Z0-9_]
      {'D', "^0-9"}, // \D → [^0-9]
      {'S', "^ \t\n\x0B\f\r"} // \S → [^ \t\n\x0B\f\r]
  };

  // List of special character class combinations that need to be handled
  // uniquely. These combinations are equivalent to matching any character,
  // including Unicode.
  std::vector<std::pair<std::string, std::string>> specialCombinations = {
      {"\\w\\W", "[\\u0000-\\U0010FFFF]"},
      {"\\W\\w", "[\\u0000-\\U0010FFFF]"},
      {"\\d\\D", "[\\u0000-\\U0010FFFF]"},
      {"\\D\\d", "[\\u0000-\\U0010FFFF]"},
      {"\\s\\S", "[\\u0000-\\U0010FFFF]"},
      {"\\S\\s", "[\\u0000-\\U0010FFFF]"}};

  std::string result;
  const char* cur = begin;
  const char* end = cur + size;
  int squareBracketCount = 0;

  while (cur < end) {
    if (*cur == '[') {
      result += *cur;
      squareBracketCount++;
      cur++;
      continue;
    }

    // Check if the current character is the end of a character class.
    if (*cur == ']' && squareBracketCount > 0) {
      result += *cur;
      squareBracketCount--;
      cur++;
      continue;
    }

    // Only process special combinations within a character class.
    if (squareBracketCount > 0) {
      bool matched = false;

      // Iterate through all special combinations to find a match.
      for (const auto& pair : specialCombinations) {
        const std::string& seq =
            pair.first; // Sequence to match (e.g., "\w\W").
        const std::string& replacement =
            pair.second; // Replacement string (e.g., "[\s\S]").
        size_t seqLen = seq.length();

        // Check if the current position matches the special sequence.
        if (cur + seqLen <= end && std::memcmp(cur, seq.data(), seqLen) == 0) {
          result += replacement;
          cur += seqLen;
          matched = true;
          break;
        }
      }

      if (matched) {
        continue;
      }
    }

    // Handle single shorthand escape sequences outside of special combinations.
    if (*cur == '\\' && (cur + 1 < end)) {
      char next_char = *(cur + 1);
      // Check if the character after '\' is one of the shorthand characters.
      if (singleShorthand.find(next_char) != singleShorthand.end()) {
        // Replace the shorthand with its explicit ICU equivalent.
        result += "[";
        result += singleShorthand[next_char];
        result += "]";
        cur += 2; // Advance past the shorthand sequence.
        continue;
      } else {
        // If it's not a shorthand character, retain the original escape
        // sequence.
        result += *cur;
        cur++;
        continue;
      }
    }

    // For all other characters, append them directly to the result.
    result += *cur;
    cur++;
  }

  return result;
}

static FOLLY_ALWAYS_INLINE std::string transDanglingRightBrackets(
    StringView input) {
  return transDanglingRightBrackets(input.begin(), input.size());
}

static FOLLY_ALWAYS_INLINE std::string transDanglingRightBrackets(
    const std::string& input) {
  return transDanglingRightBrackets(input.data(), input.size());
}

static FOLLY_ALWAYS_INLINE std::string transUnixPattern(StringView pattern) {
  static RE2 re(R"(\[\:(.*?)\:\])");
  std::string newPattern(pattern.data(), pattern.size());
  RE2::GlobalReplace(&newPattern, re, R"([:\1])");
  return newPattern;
}

static FOLLY_ALWAYS_INLINE std::string transRegexPattern(StringView pattern) {
  auto firstPattern = transUnixPattern(pattern);
  return transCharacterClass(firstPattern.data(), firstPattern.size());
}

static icu::LocalPointer<icu::RegexPattern>
compileRegexPattern(const char* begin, size_t size, uint32_t flags = 0) {
  UErrorCode status = U_ZERO_ERROR;
  icu::LocalPointer<icu::RegexPattern> regex(icu::RegexPattern::compile(
      icu::UnicodeString::fromUTF8(icu::StringPiece(begin, size)),
      flags,
      status));
  if (status == U_REGEX_RULE_SYNTAX) {
    status = U_ZERO_ERROR;
    auto transPattern = transDanglingRightBrackets(begin, size);
    regex.adoptInstead(icu::RegexPattern::compile(
        icu::UnicodeString::fromUTF8(
            icu::StringPiece(transPattern.data(), transPattern.size())),
        flags,
        status));
    if (UNLIKELY(U_FAILURE(status))) {
      BOLT_USER_FAIL(
          "failed to compile regular expression '{}', origin is '{}', error code : {}",
          transPattern,
          std::string(begin, size),
          u_errorName(status));
    }
  } else if (U_FAILURE(status)) {
    BOLT_USER_FAIL(
        "failed to compile regular expression '{}', error code : {}",
        std::string(begin, size),
        u_errorName(status));
  }
  return regex;
}

static FOLLY_ALWAYS_INLINE icu::LocalPointer<icu::RegexPattern>
compileRegexPattern(const std::string& pattern, uint32_t flags = 0) {
  return compileRegexPattern(pattern.data(), pattern.size(), flags);
}

static FOLLY_ALWAYS_INLINE icu::LocalPointer<icu::RegexPattern>
compileRegexPattern(std::string_view pattern, uint32_t flags = 0) {
  return compileRegexPattern(pattern.data(), pattern.size(), flags);
}

static FOLLY_ALWAYS_INLINE icu::LocalPointer<icu::RegexPattern>
compileRegexPattern(StringView pattern, uint32_t flags = 0) {
  return compileRegexPattern(pattern.begin(), pattern.size(), flags);
}

} // namespace bytedance::bolt::functions::regex
