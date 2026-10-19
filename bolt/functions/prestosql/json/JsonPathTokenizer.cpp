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

#include "bolt/functions/prestosql/json/JsonPathTokenizer.h"
namespace bytedance::bolt::functions {

constexpr char ROOT = '$';
constexpr char DOT = '.';
constexpr char COLON = ':';
constexpr char DASH = '-';
constexpr char QUOTE = '"';
constexpr char SINGLE_QUOTE = '\'';
constexpr char STAR = '*';
constexpr char BACK_SLASH = '\\';
constexpr char UNDER_SCORE = '_';
constexpr char OPEN_BRACKET = '[';
constexpr char CLOSE_BRACKET = ']';

constexpr char SIMDJSON_PATH_SEPERATOR = '/';

bool JsonPathTokenizer::reset(folly::StringPiece path) {
  simdJsonPathPointer_.clear();

  if (path.empty() || path[0] != ROOT) {
    return false;
  }

  index_ = 1;
  path_ = path;
  return true;
}

bool JsonPathTokenizer::hasNext() const {
  return index_ < path_.size();
}

ParseResult JsonPathTokenizer::getNext() {
  if (match(DOT)) {
    return matchDotKey();
  }
  if (match(OPEN_BRACKET)) {
    ParseResult token;
    if (match(QUOTE)) {
      token = matchQuotedSubscriptKey(QUOTE);
    } else if (match(SINGLE_QUOTE)) {
      token = matchQuotedSubscriptKey(SINGLE_QUOTE);
    } else {
      token = matchUnquotedSubscriptKey();
    }
    if (!token || !match(CLOSE_BRACKET)) {
      return folly::makeUnexpected(false);
    }
    return token;
  }
  return folly::makeUnexpected(false);
}

ParseResult JsonPathTokenizer::getSimdjsonPathPointer() const noexcept {
  return simdJsonPathPointer_;
}

bool JsonPathTokenizer::match(char expected) {
  if (index_ < path_.size() && path_[index_] == expected) {
    index_++;
    return true;
  }
  return false;
}

ParseResult JsonPathTokenizer::matchDotKey() {
  auto start = index_;
  if (match(DOT)) {
    // `..`
    return folly::makeUnexpected(false);
  } else {
    // `.*`, `.name`
    while (index_ < path_.size() && isDotKeyFormat(path_[index_])) {
      if (path_[index_] == ' ' && !spark_compatible_) {
        return folly::makeUnexpected(false);
      }
      index_++;
    }
    if (index_ <= start) {
      return folly::makeUnexpected(false);
    }

    auto res = path_.subpiece(start, index_ - start).str();
    simdJsonPathPointer_.append(1, SIMDJSON_PATH_SEPERATOR);
    simdJsonPathPointer_.append(res);
    return res;
  }
}

ParseResult JsonPathTokenizer::matchUnquotedSubscriptKey() {
  // `[*]`, `[123]`
  auto start = index_;
  while (index_ < path_.size() && isUnquotedBracketKeyFormat(path_[index_])) {
    index_++;
  }
  if (index_ <= start) {
    return folly::makeUnexpected(false);
  }
  auto res = path_.subpiece(start, index_ - start).str();
  simdJsonPathPointer_.append(1, SIMDJSON_PATH_SEPERATOR);
  simdJsonPathPointer_.append(res);
  return res;
}

// Reference Presto logic in
// src/test/java/io/prestosql/operator/scalar/TestJsonExtract.java and
// src/main/java/io/prestosql/operator/scalar/JsonExtract.java
ParseResult JsonPathTokenizer::matchQuotedSubscriptKey(char quote) {
  // `['name']`, `['*']`
  bool escaped = false;
  std::string token;

  // TODO: remove all the folly json api.
  simdJsonPathPointer_.append(1, SIMDJSON_PATH_SEPERATOR);

  while ((index_ < path_.size()) && (escaped || path_[index_] != quote)) {
    if (escaped) {
      if (path_[index_] != quote && path_[index_] != BACK_SLASH) {
        return folly::makeUnexpected(false);
      }
      escaped = false;
      token.append(1, path_[index_]);
      simdJsonPathPointer_.append(1, path_[index_]);
    } else {
      if (path_[index_] == BACK_SLASH) {
        escaped = true;

        // Only for simdjson
        simdJsonPathPointer_.append(1, path_[index_]);
      } else if (path_[index_] == quote) {
        return folly::makeUnexpected(false);
      } else {
        token.append(1, path_[index_]);
        simdJsonPathPointer_.append(1, path_[index_]);
      }
    }
    index_++;
  }
  if (escaped || token.empty() || !match(quote)) {
    return folly::makeUnexpected(false);
  }
  return token;
}

bool JsonPathTokenizer::isUnquotedBracketKeyFormat(char c) {
  return c != COLON && c != DASH && isDotKeyFormat(c);
}

bool JsonPathTokenizer::isDotKeyFormat(char c) {
  return c != OPEN_BRACKET && c != DOT && c != CLOSE_BRACKET && c != QUOTE &&
      c != SINGLE_QUOTE && c != BACK_SLASH;
}

} // namespace bytedance::bolt::functions
