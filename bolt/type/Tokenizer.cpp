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
 */
/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include "bolt/type/Tokenizer.h"
#include <folly/Unicode.h>
namespace bytedance::bolt::common {

//-------------------------------------------------------------------------
// Tokenizer static methods
//-------------------------------------------------------------------------

std::function<std::unique_ptr<Tokenizer>(const std::string&)>
    Tokenizer::tokenizerFactory_ = nullptr;

// static
std::unique_ptr<Tokenizer> Tokenizer::getInstance(const std::string& path) {
  if (!tokenizerFactory_) {
    tokenizerFactory_ = [](const std::string& p) {
      return std::make_unique<DefaultTokenizer>(p);
    };
  }
  return tokenizerFactory_(path);
}

// static
void Tokenizer::registerInstanceFactory(
    std::function<std::unique_ptr<Tokenizer>(const std::string&)>
        tokenizerFactory) {
  tokenizerFactory_ = tokenizerFactory;
}

//-------------------------------------------------------------------------
// DefaultTokenizer constructor and main interface
//-------------------------------------------------------------------------

DefaultTokenizer::DefaultTokenizer(const std::string& path)
    : path_(path), separators_(Separators::get()) {
  state = State::kNotReady;
  index_ = 0;
}

bool DefaultTokenizer::hasNext() {
  switch (state) {
    case State::kDone:
      return false;
    case State::kReady:
      return true;
    case State::kNotReady:
      break;
    case State::kFailed:
      BOLT_FAIL("Illegal state");
  }
  return tryToComputeNext();
}

std::unique_ptr<Subfield::PathElement> DefaultTokenizer::next() {
  if (!hasNext()) {
    BOLT_FAIL("No more tokens");
  }
  state = State::kNotReady;
  return std::move(next_);
}

bool DefaultTokenizer::tryToComputeNext() {
  state = State::kFailed; // temporary pessimism
  next_ = computeNext();
  if (state != State::kDone) {
    state = State::kReady;
    return true;
  }
  return false;
}

//-------------------------------------------------------------------------
// Token computation logic
//-------------------------------------------------------------------------

std::unique_ptr<Subfield::PathElement> DefaultTokenizer::computeNext() {
  if (!hasNextCharacter()) {
    state = State::kDone;
    return nullptr;
  }

  if (tryMatchSeparator(separators_->dot)) {
    std::unique_ptr<Subfield::PathElement> token = matchPathSegment();
    firstSegment = false;
    return token;
  }

  if (tryMatchSeparator(separators_->openBracket)) {
    std::unique_ptr<Subfield::PathElement> token =
        tryMatchSeparator(separators_->quote)      ? matchQuotedSubscript()
        : tryMatchSeparator(separators_->wildCard) ? matchWildcardSubscript()
                                                   : matchUnquotedSubscript();

    match(separators_->closeBracket);
    firstSegment = false;
    return token;
  }

  if (firstSegment) {
    std::unique_ptr<Subfield::PathElement> token = matchPathSegment();
    firstSegment = false;
    return token;
  }

  BOLT_UNREACHABLE();
}

//-------------------------------------------------------------------------
// Path element matching
//-------------------------------------------------------------------------

std::unique_ptr<Subfield::PathElement> DefaultTokenizer::matchPathSegment() {
  size_t start = index_;
  while (hasNextCharacter()) {
    char32_t cp = peekCodePoint();
    if (separators_->isSeparator(cp) || !isUnquotedPathCharacter(cp)) {
      break;
    }
    consumeCodePoint();
  }
  int end = index_;

  if (start == end) {
    invalidSubfieldPath();
  }
  return std::make_unique<Subfield::NestedField>(
      path_.substr(start, end - start));
}

std::unique_ptr<Subfield::PathElement>
DefaultTokenizer::matchUnquotedSubscript() {
  int start = index_;
  while (hasNextCharacter() && isUnquotedSubscriptCharacter(peekCodePoint())) {
    consumeCodePoint();
  }
  int end = index_;
  std::string token = path_.substr(start, end);

  // an empty unquoted token is not allowed
  if (token.empty()) {
    invalidSubfieldPath();
  }
  long index = 0;
  try {
    index = std::stol(token);
  } catch (...) {
    BOLT_FAIL("Invalid index {}", token);
  }
  return std::make_unique<Subfield::LongSubscript>(index);
}

std::unique_ptr<Subfield::PathElement>
DefaultTokenizer::matchQuotedSubscript() {
  // quote has already been matched

  // seek until we see the close quote
  std::string token;
  bool escaped = false;

  while (hasNextCharacter()) {
    auto [cp, len] = decodeCurrentCodePoint();

    if (escaped) {
      escaped = false;
      switch (cp) {
        case U'"':
        case U'\\':
          token += path_.substr(index_, len);
          consumeCurrentCodePoint(); // Advances index_ by len
          break;
        default:
          invalidSubfieldPath(); // Invalid escape sequence
      }
    } else if (cp == separators_->backSlash) {
      consumeCurrentCodePoint(); // Consume backslash
      escaped = true;
    } else if (cp == separators_->quote) {
      break; // Closing quote handled below
    } else {
      token += path_.substr(index_, len);
      consumeCurrentCodePoint(); // Consume normal character
    }
  }

  match(separators_->quote); // Consume closing quote
  if (token == "*") {
    return std::make_unique<Subfield::AllSubscripts>();
  }
  return std::make_unique<Subfield::StringSubscript>(token);
}

std::unique_ptr<Subfield::PathElement>
DefaultTokenizer::matchWildcardSubscript() {
  return std::make_unique<Subfield::AllSubscripts>();
}

//-------------------------------------------------------------------------
// Character classification and UTF-8 handling
//-------------------------------------------------------------------------

bool DefaultTokenizer::isUnquotedPathCharacter(char32_t cp) {
  return cp == U':' || cp == U'$' || cp == U'-' || cp == U'/' || cp == U'@' ||
      cp == U'|' || cp == U'#' || cp == U'.' ||
      isUnquotedSubscriptCharacter(cp) || (cp > 0x7F);
}

bool DefaultTokenizer::isUnquotedSubscriptCharacter(char32_t cp) {
  return (cp >= U'0' && cp <= U'9') || (cp >= U'A' && cp <= U'Z') ||
      (cp >= U'a' && cp <= U'z') || cp == U'_' || cp == U'-' || (cp > 0x7F);
}

DefaultTokenizer::CodePointInfo DefaultTokenizer::decodeCurrentCodePoint() {
  if (index_ >= path_.size()) {
    return {0, 0};
  }

  const unsigned char* p =
      reinterpret_cast<const unsigned char*>(path_.data() + index_);
  const unsigned char* e =
      reinterpret_cast<const unsigned char*>(path_.data() + path_.size());
  const unsigned char* start = p;

  bool skipOnError = false;
  try {
    char32_t cp = folly::utf8ToCodePoint(p, e, skipOnError);
    return {cp, static_cast<size_t>(p - start)};
  } catch (const std::exception&) {
    invalidSubfieldPath(); // Throws BoltRuntimeError
    return {0, 0}; // Unreachable
  }
}

char32_t DefaultTokenizer::peekCodePoint() {
  if (index_ >= path_.size()) {
    return 0;
  }
  const unsigned char* p =
      reinterpret_cast<const unsigned char*>(path_.data() + index_);
  const unsigned char* e =
      reinterpret_cast<const unsigned char*>(path_.data() + path_.size());
  const unsigned char* orig = p;
  char32_t cp = folly::utf8ToCodePoint(p, e, /*skipOnError=*/false);
  if (p == orig) {
    invalidSubfieldPath(); // Invalid UTF-8
  }
  return cp;
}

void DefaultTokenizer::consumeCodePoint() {
  if (index_ >= path_.size())
    return;
  const unsigned char* p =
      reinterpret_cast<const unsigned char*>(path_.data() + index_);
  const unsigned char* e =
      reinterpret_cast<const unsigned char*>(path_.data() + path_.size());
  const unsigned char* orig = p;
  folly::utf8ToCodePoint(p, e, /*skipOnError=*/false);
  if (p == orig) {
    invalidSubfieldPath();
  }
  index_ += (p - orig);
}

void DefaultTokenizer::consumeCurrentCodePoint() {
  if (index_ >= path_.size()) {
    return; // No-op at end of input
  }

  // Decode the current code point to get its byte length
  auto [cp, len] = decodeCurrentCodePoint();

  // Advance index by the UTF-8 code point length
  index_ += len;
}

//-------------------------------------------------------------------------
// Basic character and separator handling
//-------------------------------------------------------------------------

bool DefaultTokenizer::hasNextCharacter() {
  return index_ < path_.length();
}

bool DefaultTokenizer::tryMatchSeparator(char32_t expected) {
  return separators_->isSeparator(expected) && tryMatch(expected);
}

void DefaultTokenizer::match(char32_t expected) {
  if (!tryMatch(expected)) {
    invalidSubfieldPath();
  }
}

bool DefaultTokenizer::tryMatch(char32_t expected) {
  if (!hasNextCharacter() || peekCodePoint() != expected) {
    return false;
  }
  consumeCodePoint();
  return true;
}

//-------------------------------------------------------------------------
// Error handling
//-------------------------------------------------------------------------

void DefaultTokenizer::invalidSubfieldPath() {
  BOLT_FAIL("Invalid subfield path: {}", this->toString());
}

std::string DefaultTokenizer::toString() {
  size_t caretPos = index_;
  // Backtrack to the start of the current UTF-8 code point
  while (caretPos > 0 && (path_[caretPos] & 0xC0) == 0x80) {
    caretPos--;
  }
  return path_.substr(0, caretPos) + separators_->unicodeCaret +
      path_.substr(caretPos);
}
} // namespace bytedance::bolt::common