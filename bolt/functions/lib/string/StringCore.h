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

#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#include <unicode/brkiter.h>
#include <unicode/locid.h>
#include <unicode/regex.h>
#include <unicode/uchar.h>
#include <unicode/ustring.h>

#include <utf8proc.h>

#include <folly/CPortability.h>
#include <folly/Conv.h>
#include <folly/Range.h>
#include <folly/String.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/functions/lib/string/RegexUtils.h"
#include "bolt/type/StringView.h"

#if (ENABLE_VECTORIZATION > 0) && !defined(_DEBUG) && !defined(DEBUG)
#if defined(__clang__) && (__clang_major__ > 7)
#define IS_SANITIZER                          \
  ((__has_feature(address_sanitizer) == 1) || \
   (__has_feature(memory_sanitizer) == 1) ||  \
   (__has_feature(thread_sanitizer) == 1) ||  \
   (__has_feature(undefined_sanitizer) == 1))

#if IS_SANITIZER == 0
#define VECTORIZE_LOOP_IF_POSSIBLE _Pragma("clang loop vectorize(enable)")
#endif
#endif
#endif

#ifndef VECTORIZE_LOOP_IF_POSSIBLE
// Not supported
#define VECTORIZE_LOOP_IF_POSSIBLE
#endif
namespace bytedance::bolt::functions {
namespace stringCore {

/// Check if a given string is ascii
static bool isAscii(const char* str, size_t length);

// extended functions are not part of the original utf8proc.
static utf8proc_int32_t
utf8proc_codepoint(const char* u_input, const char* end, utf8proc_int32_t* sz);
static utf8proc_int32_t utf8proc_char_length(const char* u_input);
static utf8proc_bool utf8proc_char_first_byte(const char* u_input);
static utf8proc_int32_t utf8proc_codepoint_length(utf8proc_int32_t uc);

FOLLY_ALWAYS_INLINE bool isAscii(const char* str, size_t length) {
  for (auto i = 0; i < length; i++) {
    if (str[i] & 0x80) {
      return false;
    }
  }
  return true;
}

/// Perform reverse for ascii string input
FOLLY_ALWAYS_INLINE static void
reverseAscii(char* output, const char* input, size_t length) {
  auto j = length - 1;
  VECTORIZE_LOOP_IF_POSSIBLE for (auto i = 0; i < length; ++i, --j) {
    output[i] = input[j];
  }
}

/// Perform reverse for utf8 string input
FOLLY_ALWAYS_INLINE static void
reverseUnicode(char* output, const char* input, size_t length) {
  auto inputIdx = 0;
  auto outputIdx = length;
  while (inputIdx < length) {
    int size = 1;
    auto valid = utf8proc_codepoint(&input[inputIdx], input + length, &size);

    // if invalid utf8 gets byte sequence with nextCodePoint==-1 and size==1,
    // continue reverse invalid sequence byte by byte.
    if (valid == -1) {
      size = 1;
    }

    BOLT_USER_CHECK_GE(outputIdx, size, "access out of bound");
    outputIdx -= size;

    BOLT_USER_CHECK_LT(outputIdx, length, "access out of bound");
    std::memcpy(&output[outputIdx], &input[inputIdx], size);
    inputIdx += size;
  }
}

/// Perform upper for ascii string input
FOLLY_ALWAYS_INLINE static void
upperAscii(char* output, const char* input, size_t length) {
  VECTORIZE_LOOP_IF_POSSIBLE for (auto i = 0; i < length; i++) {
    if (input[i] >= 'a' && input[i] <= 'z') {
      output[i] = input[i] - 32;
    } else {
      output[i] = input[i];
    }
  }
}

/// Perform lower for ascii string input
FOLLY_ALWAYS_INLINE static void
lowerAscii(char* output, const char* input, size_t length) {
  VECTORIZE_LOOP_IF_POSSIBLE for (auto i = 0; i < length; i++) {
    if (input[i] >= 'A' && input[i] <= 'Z') {
      output[i] = input[i] + 32;
    } else {
      output[i] = input[i];
    }
  }
}

/// Perform upper for utf8 string input, output should be pre-allocated and
/// large enough for the results. outputLength refers to the number of bytes
/// available in the output buffer, and inputLength is the number of bytes in
/// the input string
FOLLY_ALWAYS_INLINE size_t upperUnicode(
    char* output,
    size_t outputLength,
    const char* input,
    size_t inputLength) {
  auto inputIdx = 0;
  auto outputIdx = 0;
  std::vector<char> iDotAboveChar{(char)105, (char)204, (char)135};
  utf8proc_int32_t iDotAboveUpperCodePoint = 304; // code point for İ
  utf8proc_int32_t iDotAboveLowerCodePoint = 105; // code point for i̇
  while (inputIdx < inputLength) {
    utf8proc_int32_t nextCodePoint;
    int size;
    nextCodePoint =
        utf8proc_codepoint(&input[inputIdx], input + inputLength, &size);
    if (UNLIKELY(nextCodePoint == -1)) {
      // invalid input string, copy the remaining of the input string as is to
      // the output.
      std::memcpy(&output[outputIdx], &input[inputIdx], inputLength - inputIdx);
      outputIdx += inputLength - inputIdx;
      return outputIdx;
    }
    utf8proc_int32_t upperCodePoint;
    if (nextCodePoint == iDotAboveLowerCodePoint &&
        inputIdx + iDotAboveChar.size() <= inputLength) {
      if (memcmp(
              &input[inputIdx], iDotAboveChar.data(), iDotAboveChar.size()) ==
          0) {
        upperCodePoint = iDotAboveUpperCodePoint;
        size = iDotAboveChar.size();
      } else {
        upperCodePoint = utf8proc_toupper(nextCodePoint);
      }
    } else {
      upperCodePoint = utf8proc_toupper(nextCodePoint);
    }
    inputIdx += size;

    assert(
        (outputIdx + utf8proc_codepoint_length(upperCodePoint)) <
            outputLength &&
        "access out of bound");

    auto newSize = utf8proc_encode_char(
        upperCodePoint, reinterpret_cast<unsigned char*>(&output[outputIdx]));
    outputIdx += newSize;
  }
  return outputIdx;
}

/// Perform lower for utf8 string input, output should be pre-allocated and
/// large enough for the results outputLength refers to the number of bytes
/// available in the output buffer, and inputLength is the number of bytes in
/// the input string
FOLLY_ALWAYS_INLINE size_t lowerUnicode(
    char* output,
    size_t outputLength,
    const char* input,
    size_t inputLength) {
  auto inputIdx = 0;
  auto outputIdx = 0;

  utf8proc_int32_t iDotAboveCodePoint = 304;
  std::vector<char> iDotAboveChars{(char)105, (char)204, (char)135};

  while (inputIdx < inputLength) {
    utf8proc_int32_t nextCodePoint;
    int size;
    nextCodePoint =
        utf8proc_codepoint(&input[inputIdx], input + inputLength, &size);
    if (UNLIKELY(nextCodePoint == -1)) {
      // invalid input string, copy the remaining of the input string as is to
      // the output.
      std::memcpy(&output[outputIdx], &input[inputIdx], inputLength - inputIdx);
      outputIdx += inputLength - inputIdx;
      return outputIdx;
    }

    inputIdx += size;
    utf8proc_int32_t lowerCodePoint;
    if (iDotAboveCodePoint == nextCodePoint) {
      int size = static_cast<int>(iDotAboveChars.size());
      assert((outputIdx + size) < outputLength && "access out of bound");
      std::memcpy(
          &output[outputIdx], iDotAboveChars.data(), iDotAboveChars.size());
      outputIdx += size;
      continue;
    }

    lowerCodePoint = utf8proc_tolower(nextCodePoint);

    assert(
        (outputIdx + utf8proc_codepoint_length(lowerCodePoint)) <
            outputLength &&
        "access out of bound");

    auto newSize = utf8proc_encode_char(
        lowerCodePoint, reinterpret_cast<unsigned char*>(&output[outputIdx]));
    outputIdx += newSize;
  }
  return outputIdx;
}

/// Apply a sequence of appenders to the output string sequentially.
/// @param output the output string that appenders are applied to
/// @param appenderFunc a function that appends some string to an input string
/// of type TOutStr
template <typename TOutStr, typename Func>
static void applyAppendersRecursive(TOutStr& output, Func appenderFunc) {
  appenderFunc(output);
}

template <typename TOutStr, typename Func, typename... Funcs>
static void
applyAppendersRecursive(TOutStr& output, Func appenderFunc, Funcs... funcs) {
  appenderFunc(output);
  applyAppendersRecursive(output, funcs...);
}

/**
 * Return the length in chars of a utf8 string stored in the input buffer
 * @param inputBuffer input buffer that hold the string
 * @param bufferLength size of input buffer
 * @return the number of characters represented by the input utf8 string
 */
FOLLY_ALWAYS_INLINE int64_t
lengthUnicode(const char* inputBuffer, size_t bufferLength) {
  // First address after the last byte in the buffer
  auto buffEndAddress = inputBuffer + bufferLength;
  auto currentChar = inputBuffer;
  int64_t size = 0;
  while (currentChar < buffEndAddress) {
    auto chrOffset = utf8proc_char_length(currentChar);
    // Skip bad byte if we get utf length < 0.
    currentChar += UNLIKELY(chrOffset < 0) ? 1 : chrOffset;
    size++;
  }
  return size;
}

/**
 * Return an capped length(controlled by maxChars) of a unicode string. The
 * returned length is not greater than maxChars.
 *
 * This method is used to tell whether a string is longer or the same length of
 * another string, in these scenarios we don't need accurate length, by
 * providing maxChars we can get better performance by avoid calculating whole
 * length of a string which might be very long.
 *
 * @param input input buffer that hold the string
 * @param size size of input buffer
 * @param maxChars stop counting characters if the string is longer
 * than this value
 * @return the number of characters represented by the input utf8 string
 */
FOLLY_ALWAYS_INLINE int64_t
cappedLengthUnicode(const char* input, size_t size, size_t maxChars) {
  // First address after the last byte in the input
  auto end = input + size;
  auto currentChar = input;
  int64_t numChars = 0;

  // Use maxChars to early stop to avoid calculating the whole
  // length of long string.
  while (currentChar < end && numChars < maxChars) {
    auto charSize = utf8proc_char_length(currentChar);
    // Skip bad byte if we get utf length < 0.
    currentChar += UNLIKELY(charSize < 0) ? 1 : charSize;
    numChars++;
  }

  return numChars;
}

///
/// Return an capped length in bytes(controlled by maxChars) of a unicode
/// string. The returned length may be greater than maxCharacters if there are
/// multi-byte characters present in the input string.
///
/// This method is used to help with indexing unicode strings by byte position.
/// It is used to find the byte position of the Nth character in a string.
///
/// @param input input buffer that hold the string
/// @param size size of input buffer
/// @param maxChars stop counting characters if the string is longer
/// than this value
/// @return the number of bytes represented by the input utf8 string up to
/// maxChars
///
FOLLY_ALWAYS_INLINE int64_t
cappedByteLengthUnicode(const char* input, size_t size, int64_t maxChars) {
  size_t utf8Position = 0;
  size_t numCharacters = 0;
  while (utf8Position < size && numCharacters < maxChars) {
    auto charSize = utf8proc_char_length(input + utf8Position);
    utf8Position += UNLIKELY(charSize < 0) ? 1 : charSize;
    numCharacters++;
  }
  return utf8Position;
}

/// Returns the start byte index of the Nth instance of subString in
/// string. Search starts from startPosition. Positions start with 0. If not
/// found, -1 is returned. To facilitate finding overlapping strings, the
/// nextStartPosition is incremented by 1
static inline int64_t findNthInstanceByteIndexFromStart(
    const std::string_view& string,
    const std::string_view subString,
    const size_t instance = 1,
    const size_t startPosition = 0) {
  assert(instance > 0);

  if (subString.empty()) {
    return 0;
  }

  if (startPosition >= string.size()) {
    return -1;
  }

  auto byteIndex = string.find(subString, startPosition);
  // Not found
  if (byteIndex == std::string_view::npos) {
    return -1;
  }

  // Search done
  if (instance == 1) {
    return byteIndex;
  }

  // Find next occurrence
  return findNthInstanceByteIndexFromStart(
      string, subString, instance - 1, byteIndex + 1);
}

/// Returns the start byte index of the Nth instance of subString in
/// string from the end. Search starts from endPosition. Positions start with 0.
/// If not found, -1 is returned. To facilitate finding overlapping strings, the
/// nextStartPosition is incremented by 1
inline int64_t findNthInstanceByteIndexFromEnd(
    const std::string_view string,
    const std::string_view subString,
    const size_t instance = 1) {
  assert(instance > 0);

  if (subString.empty()) {
    return 0;
  }

  size_t foundCnt = 0;
  size_t index = string.size();
  do {
    if (index == 0) {
      return -1;
    }

    index = string.rfind(subString, index - 1);
    if (index == std::string_view::npos) {
      return -1;
    }
    ++foundCnt;
  } while (foundCnt < instance);
  return index;
}

// Helper function to convert UTF-8 char index to byte index
FOLLY_ALWAYS_INLINE static int64_t
utf8CharIndexToByteIndex(const char* string, size_t strSize, size_t charIndex) {
  int64_t byteIndex = 0;
  int64_t currentCharIndex = 0;
  while (byteIndex < strSize && currentCharIndex < charIndex) {
    // Increment currentCharIndex for each starting byte of UTF-8 character
    ++currentCharIndex;

    // Determine how many bytes to skip based on the leading bits
    if ((string[byteIndex] & 0x80) == 0x00) {
      // 1 byte character
      byteIndex += 1;
    } else if ((string[byteIndex] & 0xE0) == 0xC0) {
      // 2 bytes character
      byteIndex += 2;
    } else if ((string[byteIndex] & 0xF0) == 0xE0) {
      // 3 bytes character
      byteIndex += 3;
    } else if ((string[byteIndex] & 0xF8) == 0xF0) {
      // 4 bytes character
      byteIndex += 4;
    } else {
      // Invalid UTF-8 byte, should not happen if the string is well-formed
      return -1;
    }
  }
  return byteIndex;
}

// Helper function to convert UTF-8 char index to byte index
FOLLY_ALWAYS_INLINE static int64_t utf8CharIndexToByteIndex(
    const std::string_view& string,
    size_t charIndex) {
  return utf8CharIndexToByteIndex(string.data(), string.size(), charIndex);
}

// Helper function to count UTF-8 characters up to a byte index
FOLLY_ALWAYS_INLINE static int64_t byteIndexToUtf8CharIndex(
    const std::string_view& string,
    size_t byteIndex) {
  int64_t charIndex = 0;
  for (size_t i = 0; i < byteIndex;) {
    // Increment charIndex for each starting byte of a UTF-8 character
    ++charIndex;

    // Determine how many bytes to skip based on the leading bits
    if ((string[i] & 0x80) == 0x00) {
      // 1 byte character (0xxxxxxx)
      i += 1;
    } else if ((string[i] & 0xE0) == 0xC0) {
      // 2 bytes character (110xxxxx 10xxxxxx)
      i += 2;
    } else if ((string[i] & 0xF0) == 0xE0) {
      // 3 bytes character (1110xxxx 10xxxxxx 10xxxxxx)
      i += 3;
    } else if ((string[i] & 0xF8) == 0xF0) {
      // 4 bytes character (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
      i += 4;
    } else {
      // Invalid UTF-8 byte, should not happen if the string is well-formed
      return -1;
    }
  }
  return charIndex;
}

// Helper function to convert Char-16 index to byte index
FOLLY_ALWAYS_INLINE static int64_t
char16IndexToByteIndex(const char16_t* src, size_t strSize, size_t charSize) {
  int64_t byteIndex = 0;
  const char16_t* srcLimit = src != nullptr ? src + charSize : nullptr;
  uint32_t ch = 0, ch2 = 0;
  while (byteIndex < strSize && src < srcLimit) {
    // Increment currentCharIndex for each starting byte of UTF-8 character
    uint32_t ch = *src++;
    if (ch <= 0x7f) {
      byteIndex++;
    } else if (ch <= 0x7ff) {
      byteIndex += 2;
    } else if (ch <= 0xd7ff || ch >= 0xe000) {
      byteIndex += 3;
    } else if (
        ((ch & 0x400) == 0) && src < srcLimit &&
        (((ch2 = *src) & 0xfffffc00) == 0xdc00)) {
      ++src;
      byteIndex += 4;
    } else {
      byteIndex += 3;
    }
  }
  return byteIndex;
}

/// Returns the start char index of the Nth instance of subString in
/// string. Search starts from startPosition. Positions start with 0. If not
/// found, -1 is returned. To facilitate finding overlapping strings, the
/// nextStartPosition is incremented by 1
FOLLY_ALWAYS_INLINE static int64_t findNthInstanceCharIndexFromStart(
    const std::string_view& string,
    const std::string_view subString,
    const size_t instance = 1,
    const size_t utf8StartPosition = 0) {
  assert(instance > 0);

  // Convert UTF-8 char index to byte index
  int64_t byteStartPosition =
      utf8CharIndexToByteIndex(string, utf8StartPosition);
  if (UNLIKELY(byteStartPosition < 0)) {
    return -1;
  }

  // Find byte index of the substring
  int64_t byteIndex = findNthInstanceByteIndexFromStart(
      string, subString, instance, byteStartPosition);

  // Convert byte index back to UTF-8 char index
  if (byteIndex != -1) {
    return byteIndexToUtf8CharIndex(string, byteIndex);
  } else {
    return -1; // Not found
  }
}

/// Replace replaced with replacement in inputString and write results in
/// outputString. If inPlace=true inputString and outputString are assumed to
/// tbe the same. When replaced is empty and ignoreEmptyReplaced is false,
/// replacement is added before and after each charecter. When replaced is
/// empty and ignoreEmptyReplaced is true, the result is the inputString value.
/// When inputString is empty results is empty.
/// replace("", "", "x") = ""
/// replace("aa", "", "x") = "xaxax" -- when ignoreEmptyReplaced is false
/// replace("aa", "", "x") = "aa" -- when ignoreEmptyReplaced is true
template <bool ignoreEmptyReplaced = false>
inline static size_t replace(
    char* outputString,
    const std::string_view& inputString,
    const std::string_view& replaced,
    const std::string_view& replacement,
    bool inPlace = false) {
  if (inputString.size() == 0) {
    return 0;
  }

  if constexpr (ignoreEmptyReplaced) {
    if (replaced.size() == 0) {
      if (!inPlace) {
        std::memcpy(outputString, inputString.data(), inputString.size());
      }
      return inputString.size();
    }
  }

  size_t readPosition = 0;
  size_t writePosition = 0;
  // Copy needed in out of place replace, and when replaced and replacement are
  // of different sizes.
  bool doCopyUnreplaced = !inPlace || (replaced.size() != replacement.size());

  auto findNextReplaced = [&]() {
    return findNthInstanceByteIndexFromStart(
        inputString, replaced, 1, readPosition);
  };

  auto writeUnchanged = [&](ssize_t size) {
    assert(size >= 0 && "Probable math error?");
    if (size <= 0) {
      return;
    }

    if (inPlace) {
      if (doCopyUnreplaced) {
        // memcpy does not allow overllapping
        std::memmove(
            &outputString[writePosition],
            &inputString.data()[readPosition],
            size);
      }
    } else {
      std::memcpy(
          &outputString[writePosition],
          &inputString.data()[readPosition],
          size);
    }
    writePosition += size;
    readPosition += size;
  };

  auto writeReplacement = [&]() {
    if (replacement.size() > 0) {
      std::memcpy(
          &outputString[writePosition], replacement.data(), replacement.size());
      writePosition += replacement.size();
    }
    readPosition += replaced.size();
  };

  // Special case when size of replaced is 0
  if (replaced.size() == 0) {
    if (replacement.size() == 0) {
      if (!inPlace) {
        std::memcpy(outputString, inputString.data(), inputString.size());
      }
      return inputString.size();
    }

    // Can never be in place since replacement.size()>replaced.size()
    assert(!inPlace && "wrong inplace replace usage");

    // add replacement before and after each char in inputString
    for (auto i = 0; i < inputString.size(); i++) {
      writeReplacement();

      outputString[writePosition] = inputString[i];
      writePosition++;
    }

    writeReplacement();
    return writePosition;
  }

  while (readPosition < inputString.size()) {
    // Find next token to replace
    auto position = findNextReplaced();

    if (position == -1) {
      break;
    }
    assert(position >= 0 && "invalid position found");
    auto unchangedSize = position - readPosition;
    writeUnchanged(unchangedSize);
    writeReplacement();
  }

  auto unchangedSize = inputString.size() - readPosition;
  writeUnchanged(unchangedSize);

  return writePosition;
}

/// Given a utf8 string, a starting position and length returns the
/// corresponding underlying byte range [startByteIndex, endByteIndex).
/// Byte indicies starts from 0, UTF8 character positions starts from 1.
/// If a bad unicode byte is encountered, then we skip that bad byte and
/// count that as one codepoint.
template <bool isAscii>
static inline std::pair<size_t, size_t>
getByteRange(const char* str, size_t startCharPosition, size_t length) {
  if (startCharPosition < 1 && length > 0) {
    throw std::invalid_argument(
        "start position must be >= 1 and length must be > 0");
  }
  if constexpr (isAscii) {
    return std::make_pair(
        startCharPosition - 1, startCharPosition + length - 1);
  } else {
    size_t startByteIndex = 0;
    size_t nextCharOffset = 0;

    // Find startByteIndex
    for (auto i = 0; i < startCharPosition - 1; i++) {
      auto increment = utf8proc_char_length(&str[nextCharOffset]);
      nextCharOffset += UNLIKELY(increment < 0) ? 1 : increment;
    }

    startByteIndex = nextCharOffset;

    // Find endByteIndex
    for (auto i = 0; i < length; i++) {
      auto increment = utf8proc_char_length(&str[nextCharOffset]);
      nextCharOffset += UNLIKELY(increment < 0) ? 1 : increment;
    }

    return std::make_pair(startByteIndex, nextCharOffset);
  }
}

// Converts a Unicode code point to UTF-16 surrogate pair escape sequences.
static std::string codepointToUtf16SurrogatePairs(uint32_t codepoint) {
  // Calculate the high and low surrogate pairs
  uint32_t highSurrogate = 0xD800 + ((codepoint - 0x10000) >> 10);
  uint32_t lowSurrogate = 0xDC00 + (codepoint & 0x3FF);

  // Allocate a buffer for the formatted output
  char buffer[13]; // Enough space for "\\uXXXX\\uXXXX" plus null terminator

  // Format the surrogate pairs into the buffer using snprintf
  snprintf(
      buffer,
      sizeof(buffer),
      "\\u%04X\\u%04X",
      static_cast<uint16_t>(highSurrogate),
      static_cast<uint16_t>(lowSurrogate));

  // Create a string from the buffer
  return std::string(buffer);
}

// Decodes a single UTF-8 encoded character into a Unicode code point.
static bool decodeUtf8ToCodepoint(
    const std::string& utf8str,
    size_t& index,
    uint32_t& codepoint) {
  size_t remainingBytes =
      0; // Number of bytes remaining in a UTF-8 character sequence.
  codepoint = 0;

  if ((utf8str[index] & 0x80) == 0x00) {
    codepoint = utf8str[index];
    remainingBytes = 0;
  } else if ((utf8str[index] & 0xE0) == 0xC0) {
    codepoint = utf8str[index] & 0x1F;
    remainingBytes = 1;
  } else if ((utf8str[index] & 0xF0) == 0xE0) {
    codepoint = utf8str[index] & 0x0F;
    remainingBytes = 2;
  } else if ((utf8str[index] & 0xF8) == 0xF0) {
    codepoint = utf8str[index] & 0x07;
    remainingBytes = 3;
  } else {
    // Invalid UTF-8 starting byte
    return false;
  }

  index++;

  while (remainingBytes > 0) {
    if (index >= utf8str.size() || (utf8str[index] & 0xC0) != 0x80) {
      // Invalid UTF-8 continuation byte
      return false;
    }
    codepoint = (codepoint << 6) | (utf8str[index] & 0x3F);
    index++;
    remainingBytes--;
  }

  // Code point validity check
  if (codepoint > 0x10FFFF || (codepoint >= 0xD800 && codepoint <= 0xDFFF)) {
    // Beyond Unicode range or a surrogate code point
    return false;
  }

  return true;
}

// Replaces non-BMP characters with Unicode escape sequences in a string.
static std::string replaceNonBMPWithUnicodeSequence(
    const std::string& utf8str) {
  std::string result;
  size_t expectedSize = utf8str.size();
  result.reserve(expectedSize); // Reserve the original size to start with.

  size_t index = 0;

  while (index < utf8str.size()) {
    uint32_t codepoint = 0;
    size_t startIndex = index;

    if (!decodeUtf8ToCodepoint(utf8str, index, codepoint)) {
      // Handle invalid UTF-8 sequences
      return utf8str;
    }

    if (codepoint > 0xFFFF) {
      // Encountered a non-BMP character
      std::string surrogatePair = codepointToUtf16SurrogatePairs(codepoint);
      if (result.empty()) {
        // If it's the first replacement, initialize result with the processed
        // part of the string
        result = utf8str.substr(0, startIndex);
        expectedSize +=
            surrogatePair
                .size(); // Adjust the expected size for the surrogate pair
        result.reserve(expectedSize);
      }

      result += surrogatePair; // Append the escape sequence
    } else {
      // BMP character or ASCII
      if (!result.empty()) {
        // If a replacement has occurred, append the character directly to the
        // result
        result += utf8str.substr(startIndex, index - startIndex);
      }
    }
  }

  // If no replacements occurred, return the original string
  return result.empty() ? utf8str : result;
}

// check whether the inputPattern is a common regex pattern (or illegal
// pattern), if so, return true else return false and the pattern removing the
// escape char, for regex_metachars like "\\|", "\\.", "\\|\\|", "'a'".
// Only the common pattern takes the fast path, rather than ICU.
static bool checkRegexPattern(
    std::string& inputPattern,
    bool matchDanglingRightBrackets = true) {
  std::string pattern = inputPattern;
  if (matchDanglingRightBrackets) {
    pattern = regex::transDanglingRightBrackets(inputPattern);
  }

  // remove "\\" for regex_metachars
  std::string new_pattern;
  const std::vector<char> regex_metachars = {
      '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\'};

  bool escape = false;
  for (char c : pattern) {
    if (escape) {
      if (std::find(regex_metachars.begin(), regex_metachars.end(), c) !=
          regex_metachars.end()) { // remove "\\" for regex_metachars
        new_pattern.append(1, c);
        escape = false;
        continue;
      } else { // such as: "\\a", "\\x"
        return true;
      }
    }

    if (c == '\\') {
      escape = true;
      continue;
    }

    if (std::find(regex_metachars.begin(), regex_metachars.end(), c) !=
        regex_metachars.end()) {
      return true;
    } else {
      new_pattern.append(1, c);
    }
  }
  if (escape) { // tailing "\\"
    return true;
  }
  inputPattern = new_pattern;
  return false;
}

FOLLY_ALWAYS_INLINE static bool isAlpha(const char* str, size_t length) {
  if (str == nullptr || length == 0) {
    return false;
  }

  bool allAsciiAlpha = true;
  for (size_t i = 0; i < length; i++) {
    auto c = static_cast<unsigned char>(str[i]);
    if (c > 127) {
      allAsciiAlpha = false;
      break;
    }
    if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))) {
      return false;
    }
  }
  if (allAsciiAlpha) {
    return true;
  }
  int32_t i = 0;

  while (i < length) {
    UChar32 c;
    U8_NEXT(str, i, length, c);
    if (c < 0) {
      return false;
    }
    if (!u_isalpha(c)) {
      return false;
    }
  }
  return true;
}

FOLLY_ALWAYS_INLINE static bool isAlpha(const std::string& str) {
  return isAlpha(str.data(), str.size());
}

FOLLY_ALWAYS_INLINE static bool isAlpha(const std::string_view& str) {
  return isAlpha(str.data(), str.size());
}

FOLLY_ALWAYS_INLINE static bool isAlpha(const StringView& str) {
  return isAlpha(str.data(), str.size());
}

FOLLY_ALWAYS_INLINE static bool isDigit(const char* str, size_t length) {
  if (str == nullptr || length == 0) {
    return false;
  }

  bool allAsciiDigit = true;
  for (size_t i = 0; i < length; i++) {
    auto c = static_cast<unsigned char>(str[i]);
    if (c > 127) {
      allAsciiDigit = false;
      break;
    }
    if (!(c >= '0' && c <= '9')) {
      return false;
    }
  }

  if (allAsciiDigit) {
    return true;
  }

  int32_t i = 0;
  while (i < length) {
    UChar32 c;
    U8_NEXT(str, i, length, c);
    if (c < 0) {
      return false;
    }
    if (!u_isdigit(c)) {
      return false;
    }
  }
  return true;
}

FOLLY_ALWAYS_INLINE static bool isDigit(const std::string& str) {
  return isDigit(str.data(), str.size());
}

FOLLY_ALWAYS_INLINE static bool isDigit(const std::string_view& str) {
  return isDigit(str.data(), str.size());
}

FOLLY_ALWAYS_INLINE static bool isDigit(const StringView& str) {
  return isDigit(str.data(), str.size());
}

FOLLY_ALWAYS_INLINE static bool isDecimal(const char* str, size_t length) {
  if (str == nullptr || length == 0) {
    return false;
  }
  folly::StringPiece sv(str, length);
  auto l = folly::tryTo<long>(sv);
  if (l.hasValue()) {
    return true;
  }

  auto ul = folly::tryTo<unsigned long>(sv);
  if (ul.hasValue()) {
    return true;
  }

  auto d = folly::tryTo<double>(sv);
  return d.hasValue() && !std::isnan(d.value()) && !std::isinf(d.value());
}

FOLLY_ALWAYS_INLINE static bool isDecimal(const std::string& str) {
  return isDecimal(str.data(), str.size());
}

FOLLY_ALWAYS_INLINE static bool isDecimal(const std::string_view& str) {
  return isDecimal(str.data(), str.size());
}

FOLLY_ALWAYS_INLINE static bool isDecimal(const StringView& str) {
  return isDecimal(str.data(), str.size());
}

FOLLY_ALWAYS_INLINE static const icu::Locale& getDefaultLocale() {
  static const icu::Locale locale = icu::Locale::getRoot();
  return locale;
}

template <bool isAscii = true>
FOLLY_ALWAYS_INLINE static std::string toLower(
    const char* str,
    size_t length,
    const icu::Locale& locale = getDefaultLocale()) {
  if (str == nullptr || length == 0) {
    return "";
  }
  std::string result;
  result.resize(length);
  if constexpr (isAscii) {
    bool allAscii = true;
    for (size_t i = 0; i < length; i++) {
      auto c = static_cast<unsigned char>(str[i]);
      if (c > 127) {
        allAscii = false;
        break;
      }
      result[i] = std::tolower(c);
    }
    if (allAscii) {
      return result;
    }
  }
  icu::UnicodeString unicodeStr =
      icu::UnicodeString::fromUTF8({str, static_cast<int32_t>(length)});
  unicodeStr.toLower(locale);
  result.clear();
  unicodeStr.toUTF8String(result);
  return result;
}

template <bool isAscii = true>
FOLLY_ALWAYS_INLINE static std::string toLower(
    const std::string& str,
    const icu::Locale& locale = getDefaultLocale()) {
  return toLower<isAscii>(str.data(), str.size(), locale);
}

template <bool isAscii = true>
FOLLY_ALWAYS_INLINE static std::string toLower(
    const std::string_view& str,
    const icu::Locale& locale = getDefaultLocale()) {
  return toLower<isAscii>(str.data(), str.size(), locale);
}

template <bool isAscii = true>
FOLLY_ALWAYS_INLINE static std::string toLower(
    const StringView& str,
    const icu::Locale& locale = getDefaultLocale()) {
  return toLower<isAscii>(str.data(), str.size(), locale);
}

template <bool isAscii = true>
FOLLY_ALWAYS_INLINE static std::string toUpper(
    const char* str,
    size_t length,
    const icu::Locale& locale = getDefaultLocale()) {
  if (str == nullptr || length == 0) {
    return "";
  }
  std::string result;
  result.resize(length);
  if constexpr (isAscii) {
    bool allAscii = true;
    for (size_t i = 0; i < length; i++) {
      auto c = static_cast<unsigned char>(str[i]);
      if (c > 127) {
        allAscii = false;
        break;
      }
      result[i] = std::toupper(c);
    }
    if (allAscii) {
      return result;
    }
  }
  icu::UnicodeString unicodeStr =
      icu::UnicodeString::fromUTF8({str, static_cast<int32_t>(length)});
  unicodeStr.toUpper(locale);
  result.clear();
  unicodeStr.toUTF8String(result);
  return result;
}

template <bool isAscii = true>
FOLLY_ALWAYS_INLINE static std::string toUpper(
    const std::string& str,
    const icu::Locale& locale = getDefaultLocale()) {
  return toUpper<isAscii>(str.data(), str.size(), locale);
}

template <bool isAscii = true>
FOLLY_ALWAYS_INLINE static std::string toUpper(
    const std::string_view& str,
    const icu::Locale& locale = getDefaultLocale()) {
  return toUpper<isAscii>(str.data(), str.size(), locale);
}

template <bool isAscii = true>
FOLLY_ALWAYS_INLINE static std::string toUpper(
    const StringView& str,
    const icu::Locale& locale = getDefaultLocale()) {
  return toUpper<isAscii>(str.data(), str.size(), locale);
}

FOLLY_ALWAYS_INLINE static std::string toTitle(
    const char* str,
    size_t length,
    const icu::Locale& locale = getDefaultLocale()) {
  if (str == nullptr || length == 0) {
    return "";
  }
  UErrorCode status = U_ZERO_ERROR;
  icu::UnicodeString unicodeStr =
      icu::UnicodeString::fromUTF8({str, static_cast<int32_t>(length)});

  std::unique_ptr<icu::BreakIterator> bi(
      icu::BreakIterator::createWordInstance(locale, status));
  BOLT_USER_CHECK(
      U_SUCCESS(status),
      "Failed to create BreakIterator: {}, for locale: {}, Data Path: {}",
      u_errorName(status),
      locale.getName(),
      u_getDataDirectory());
  auto title = unicodeStr.toTitle(bi.get(), locale, 0);
  std::string output;
  title.toUTF8String(output);
  return output;
}

FOLLY_ALWAYS_INLINE static std::string toTitle(
    const std::string& str,
    const icu::Locale& locale = getDefaultLocale()) {
  return toTitle(str.data(), str.size(), locale);
}

FOLLY_ALWAYS_INLINE static std::string toTitle(
    const std::string_view& str,
    const icu::Locale& locale = getDefaultLocale()) {
  return toTitle(str.data(), str.size(), locale);
}

FOLLY_ALWAYS_INLINE static std::string toTitle(
    const StringView& str,
    const icu::Locale& locale = getDefaultLocale()) {
  return toTitle(str.data(), str.size(), locale);
}

// This function is not part of the utf8proc, it copy it from duckdb.cpp
// it should be faster than utf8proc_iterate
// from http://www.zedwood.com/article/cpp-utf8-char-to-codepoint
// `end` is a pointer to the first byte past the end of the string.
FOLLY_ALWAYS_INLINE utf8proc_int32_t
utf8proc_codepoint(const char* u_input, const char* end, utf8proc_int32_t* sz) {
  const unsigned char* u = (const unsigned char*)u_input;
  unsigned char u0 = u[0];
  if (u0 <= 127) {
    *sz = 1;
    return u0;
  }
  if (end - u_input < 2) {
    return -1;
  }
  unsigned char u1 = u[1];
  if (u0 >= 192 && u0 <= 223) {
    *sz = 2;
    return (u0 - 192) * 64 + (u1 - 128);
  }
  if (u[0] == 0xed && (u[1] & 0xa0) == 0xa0) {
    return -1; // code points, 0xd800 to 0xdfff
  }
  if (end - u_input < 3) {
    return -1;
  }
  unsigned char u2 = u[2];
  if (u0 >= 224 && u0 <= 239) {
    *sz = 3;
    return (u0 - 224) * 4096 + (u1 - 128) * 64 + (u2 - 128);
  }
  if (end - u_input < 4) {
    return -1;
  }
  unsigned char u3 = u[3];
  if (u0 >= 240 && u0 <= 247) {
    *sz = 4;
    return (u0 - 240) * 262144 + (u1 - 128) * 4096 + (u2 - 128) * 64 +
        (u3 - 128);
  }
  return -1;
}

/// Return the size in bytes for the char pointed to by u_input.
/// This function is not part of the original utf8proc, it is a simplified
/// verion of utf8proc_codepoint. It assumes a valid utf8 input otherwise output
/// is undefined.
FOLLY_ALWAYS_INLINE utf8proc_int32_t utf8proc_char_length(const char* u_input) {
  const unsigned char* u = (const unsigned char*)u_input;
  unsigned char u0 = u[0];
  if (u0 <= 127) {
    return 1;
  }

  if (u0 >= 192 && u0 <= 223) {
    return 2;
  }

  if (u0 >= 224 && u0 <= 239) {
    return 3;
  }

  if (u0 >= 240 && u0 <= 247) {
    return 4;
  }
  return -1;
}

/// This function is not part of the original utf8proc.
/// A utf-8 character may be 1 to 4 bytes long. The function will determine if
/// the input is pointing to the first byte.
FOLLY_ALWAYS_INLINE utf8proc_bool
utf8proc_char_first_byte(const char* u_input) {
  const unsigned char* u = (const unsigned char*)u_input;
  unsigned char u0 = u[0];
  return u0 <= 127 || u0 >= 192;
}

/// Return the size in bytes for the char represented by the input code point.
/// The output is not undefined if uc is invalid. Its implemented to match
/// the space writen by utf8proc_encode_char for invalid inputs case.
FOLLY_ALWAYS_INLINE utf8proc_int32_t
utf8proc_codepoint_length(utf8proc_int32_t uc) {
  if (uc < 0x00) {
    return 0;
  } else if (uc < 0x80) {
    return 1;
  } else if (uc < 0x800) {
    return 2;
    // Note: utf8proc_encode_char allow encoding 0xd800-0xdfff here, so as not
    // to change the API, however, these are actually invalid in UTF-8
  } else if (uc < 0x10000) {
    return 3;
  } else if (uc < 0x110000) {
    return 4;
  } else {
    return 0;
  }
}

} // namespace stringCore
} // namespace bytedance::bolt::functions
