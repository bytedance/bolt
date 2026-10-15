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

#include <folly/Expected.h>
#include <folly/Range.h>
namespace bytedance::bolt::functions {

using ParseResult = folly::Expected<std::string, bool>;

class JsonPathTokenizer {
 public:
  explicit JsonPathTokenizer(bool spark_compatible)
      : spark_compatible_(spark_compatible){};
  bool reset(folly::StringPiece path);

  bool hasNext() const;

  ParseResult getNext();

  ParseResult getSimdjsonPathPointer() const noexcept;

 private:
  bool match(char expected);

  ParseResult matchDotKey();

  ParseResult matchUnquotedSubscriptKey();

  ParseResult matchQuotedSubscriptKey(char quote);

  bool isUnquotedBracketKeyFormat(char c);

  bool isDotKeyFormat(char c);

 private:
  size_t index_{0};
  folly::StringPiece path_;

  std::string simdJsonPathPointer_;

  const bool spark_compatible_;
};

} // namespace bytedance::bolt::functions
