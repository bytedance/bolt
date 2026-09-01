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

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>

#include <folly/CPortability.h>
#include <folly/small_vector.h>
#include <unicode/brkiter.h>
#include <unicode/locid.h>
#include <unicode/uchar.h>
#include <unicode/unistr.h>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::functions::stringCore::spark {

FOLLY_ALWAYS_INLINE bool isJavaNumber(UChar32 codePoint) {
  // Java groups all three Unicode number categories into its numeric word
  // rules. This is also used to distinguish A1-Σ from AΣ-B below.
  const auto category = u_charType(codePoint);
  return category == U_DECIMAL_DIGIT_NUMBER || category == U_LETTER_NUMBER ||
      category == U_OTHER_NUMBER;
}

FOLLY_ALWAYS_INLINE bool isJavaCased(UChar32 codePoint) {
  // Java 11 uses Unicode 10, where Georgian Mkhedruli is an uncased Lo
  // script. ICU 74 uses Unicode 15.1, where it is classified as Ll. Hence
  // Java requires AΣა -> aςა, while ICU's Cased property produces aσა.
  if ((codePoint >= 0x10D0 && codePoint <= 0x10FA) ||
      (codePoint >= 0x10FD && codePoint <= 0x10FF)) {
    return false;
  }

  const auto category = u_charType(codePoint);
  return category == U_LOWERCASE_LETTER || category == U_UPPERCASE_LETTER ||
      category == U_TITLECASE_LETTER ||
      (codePoint >= 0x02B0 && codePoint <= 0x02B8) ||
      (codePoint >= 0x02C0 && codePoint <= 0x02C1) ||
      (codePoint >= 0x02E0 && codePoint <= 0x02E4) || codePoint == 0x0345 ||
      codePoint == 0x037A || (codePoint >= 0x1D2C && codePoint <= 0x1D61) ||
      (codePoint >= 0x2160 && codePoint <= 0x217F) ||
      (codePoint >= 0x24B6 && codePoint <= 0x24E9);
}

FOLLY_ALWAYS_INLINE bool isJavaWordBoundary(
    const icu::UnicodeString& input,
    int32_t offset,
    icu::BreakIterator& breakIterator) {
  if (offset == 0 || offset == input.length()) {
    return true;
  }

  const auto previousOffset = input.moveIndex32(offset, -1);
  const auto previousCodePoint = input.char32At(previousOffset);
  const auto nextCodePoint = input.char32At(offset);

  // ICU 74 does not report these boundaries, but Java 11 treats both code
  // points as word separators:
  //   U+00B7: AΣ·B -> aς·b.
  //   U+202F: AΣ\u202FB -> aς\u202Fb.
  const auto forcesBoundary = [](UChar32 codePoint) {
    return codePoint == 0x00B7 || codePoint == 0x202F;
  };
  if (forcesBoundary(previousCodePoint) || forcesBoundary(nextCodePoint)) {
    return true;
  }

  // A hyphen joins Java words in AΣ-B -> aσ-b, but a hyphen following a
  // number does not connect that number to the word on its right:
  // A1-Σ -> a1-σ.
  if (previousCodePoint == '-' && previousOffset > 0) {
    const auto beforeHyphenOffset = input.moveIndex32(previousOffset, -1);
    if (isJavaNumber(input.char32At(beforeHyphenOffset))) {
      return true;
    }
  }

  if (!breakIterator.isBoundary(offset)) {
    return false;
  }

  // Remove boundaries that ICU 74 reports but Java 11 bridges:
  //   Number: AΣ²B -> aσ²b.
  //   Format: AΣ\u200BB -> aσ\u200Bb.
  //   OtherLetter: AΣ가B -> aσ가b and AΣ㐀B -> aσ㐀b.
  //   Hyphen: AΣ-B -> aσ-b (subject to the numeric override above).
  const auto bridgesBoundary = [](UChar32 codePoint) {
    const auto category = u_charType(codePoint);
    return codePoint == '"' || codePoint == '-' ||
        category == U_DECIMAL_DIGIT_NUMBER || category == U_LETTER_NUMBER ||
        category == U_OTHER_NUMBER || category == U_FORMAT_CHAR ||
        category == U_OTHER_LETTER;
  };
  return !bridgesBoundary(previousCodePoint) && !bridgesBoundary(nextCodePoint);
}

FOLLY_ALWAYS_INLINE bool isJavaFinalSigma(
    const icu::UnicodeString& input,
    int32_t sigmaOffset,
    icu::BreakIterator& breakIterator) {
  auto offset = sigmaOffset;
  while (offset >= 0 && !isJavaWordBoundary(input, offset, breakIterator)) {
    const auto codePoint = input.char32At(offset - 1);
    if (isJavaCased(codePoint)) {
      offset = sigmaOffset + 1;
      while (offset < input.length() &&
             !isJavaWordBoundary(input, offset, breakIterator)) {
        const auto followingCodePoint = input.char32At(offset);
        if (isJavaCased(followingCodePoint)) {
          return false;
        }
        offset += U16_LENGTH(followingCodePoint);
      }
      return true;
    }
    offset -= U16_LENGTH(codePoint);
  }
  return false;
}

/// Holds one word break iterator per thread. BreakIterator::setText retains a
/// reference to its input, hence the iterator is rebound to emptyText_ between
/// calls before the caller's UnicodeString is modified or destroyed.
class SparkBreakIteratorHolder {
 public:
  SparkBreakIteratorHolder() {
    UErrorCode status = U_ZERO_ERROR;
    const auto& locale = icu::Locale::getRoot();
    breakIterator_.reset(
        icu::BreakIterator::createWordInstance(locale, status));
    BOLT_USER_CHECK(
        U_SUCCESS(status) && breakIterator_ != nullptr,
        "Failed to create BreakIterator: {}, for locale: {}, Data Path: {}",
        u_errorName(status),
        locale.getName(),
        u_getDataDirectory());
    breakIterator_->setText(emptyText_);
  }

  icu::BreakIterator& breakIterator() {
    return *breakIterator_;
  }

  const icu::UnicodeString& emptyText() const {
    return emptyText_;
  }

 private:
  // Members are destroyed in reverse declaration order. Keep emptyText_ alive
  // until after breakIterator_ releases its retained text reference.
  icu::UnicodeString emptyText_;
  std::unique_ptr<icu::BreakIterator> breakIterator_;
};

FOLLY_ALWAYS_INLINE SparkBreakIteratorHolder& sparkBreakIteratorHolder() {
  static thread_local SparkBreakIteratorHolder holder;
  return holder;
}

class ScopedBreakIteratorText {
 public:
  ScopedBreakIteratorText(
      SparkBreakIteratorHolder& holder,
      const icu::UnicodeString& input)
      : holder_(holder) {
    holder_.breakIterator().setText(input);
  }

  ~ScopedBreakIteratorText() {
    holder_.breakIterator().setText(holder_.emptyText());
  }

  ScopedBreakIteratorText(const ScopedBreakIteratorText&) = delete;
  ScopedBreakIteratorText& operator=(const ScopedBreakIteratorText&) = delete;

 private:
  SparkBreakIteratorHolder& holder_;
};

FOLLY_ALWAYS_INLINE void adjustJavaSigmaInPlace(
    icu::UnicodeString& input,
    int32_t sigmaOffset) {
  struct Replacement {
    int32_t offset;
    char16_t codePoint;
  };

  folly::small_vector<Replacement, 4> replacements;
  {
    auto& holder = sparkBreakIteratorHolder();
    ScopedBreakIteratorText scopedText(holder, input);
    auto& breakIterator = holder.breakIterator();
    do {
      replacements.push_back(
          {sigmaOffset,
           isJavaFinalSigma(input, sigmaOffset, breakIterator)
               ? char16_t{0x03C2}
               : char16_t{0x03C3}});
      sigmaOffset = input.indexOf(0x03A3, sigmaOffset + 1);
    } while (sigmaOffset >= 0);
  }

  // BreakIterator no longer references input. It is now safe to mutate it.
  for (const auto& replacement : replacements) {
    input.setCharAt(replacement.offset, replacement.codePoint);
  }
}

} // namespace bytedance::bolt::functions::stringCore::spark
