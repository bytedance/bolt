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

#include <vector>

#include "gtest/gtest.h"

using namespace std::string_literals;
using bytedance::bolt::functions::JsonPathTokenizer;
using TokenList = std::vector<std::string>;

#define EXPECT_TOKEN_EQ(path, expected)  \
  {                                      \
    auto jsonPath = path;                \
    auto tokens = getTokens(jsonPath);   \
    EXPECT_TRUE(bool(tokens));           \
    EXPECT_EQ(expected, tokens.value()); \
  }

#define EXPECT_TOKEN_INVALID(path) \
  {                                \
    auto tokens = getTokens(path); \
    EXPECT_FALSE(bool(tokens));    \
  }

#define EXPECT_QUOTED_TOKEN_EQ(path, expected) \
  {                                            \
    auto quotedPath = "$[\"" + path + "\"]";   \
    EXPECT_TOKEN_EQ(quotedPath, expected);     \
  }

#define EXPECT_UNQUOTED_TOKEN_INVALID(path) \
  {                                         \
    auto invalidPath = "$." + path;         \
    EXPECT_TOKEN_INVALID(invalidPath);      \
  }

// The test is ported from Presto for compatibility
folly::Expected<TokenList, bool> getTokens(const std::string& path) {
  JsonPathTokenizer tokenizer;
  tokenizer.reset(path);
  TokenList tokens;
  while (tokenizer.hasNext()) {
    if (auto token = tokenizer.getNext()) {
      tokens.push_back(token.value());
    } else {
      tokens.clear();
      return folly::makeUnexpected(false);
    }
  }
  return tokens;
}

TEST(JsonPathTokenizerTest, tokenizeTest) {
  EXPECT_TOKEN_EQ("$"s, TokenList());
  EXPECT_TOKEN_EQ("$.foo"s, TokenList{"foo"s});
  EXPECT_TOKEN_EQ("$[\"foo\"]"s, TokenList{"foo"s});
  EXPECT_TOKEN_EQ("$[\"foo.bar\"]"s, TokenList{"foo.bar"s});
  EXPECT_TOKEN_EQ("$[42]"s, TokenList{"42"s});
  EXPECT_TOKEN_EQ("$.42"s, TokenList{"42"s});
  EXPECT_TOKEN_EQ("$.42.63"s, (TokenList{"42"s, "63"s}));
  EXPECT_TOKEN_EQ(
      "$.foo.42.bar.63"s, (TokenList{"foo"s, "42"s, "bar"s, "63"s}));
  EXPECT_TOKEN_EQ("$.x.foo"s, (TokenList{"x"s, "foo"s}));
  EXPECT_TOKEN_EQ("$.x[\"foo\"]"s, (TokenList{"x"s, "foo"s}));
  EXPECT_TOKEN_EQ("$.x[42]"s, (TokenList{"x"s, "42"s}));
  EXPECT_TOKEN_EQ("$.foo_42._bar63"s, (TokenList{"foo_42"s, "_bar63"s}));
  EXPECT_TOKEN_EQ("$[foo_42][_bar63]"s, (TokenList{"foo_42"s, "_bar63"s}));
  EXPECT_TOKEN_EQ("$.foo:42.:bar63"s, (TokenList{"foo:42"s, ":bar63"s}));
  EXPECT_TOKEN_EQ(
      "$[\"foo:42\"][\":bar63\"]"s, (TokenList{"foo:42"s, ":bar63"s}));
  EXPECT_TOKEN_EQ(
      "$.store.fruit[*].weight",
      (TokenList{"store"s, "fruit"s, "*"s, "weight"s}));
  EXPECT_QUOTED_TOKEN_EQ("!@#$%^&*()[]{}/?'"s, TokenList{"!@#$%^&*()[]{}/?'"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("!@#$%^&*()[]{}/?'"s);
  EXPECT_QUOTED_TOKEN_EQ("ab\u0001c"s, TokenList{"ab\u0001c"s});
  EXPECT_TOKEN_EQ("$.ab\u0001c"s, TokenList{"ab\u0001c"s});
  EXPECT_QUOTED_TOKEN_EQ("ab\0c"s, TokenList{"ab\0c"s});
  EXPECT_TOKEN_EQ("$.ab\0c"s, TokenList{"ab\0c"s});
  EXPECT_QUOTED_TOKEN_EQ("ab\t\n\rc"s, TokenList{"ab\t\n\rc"s});
  EXPECT_TOKEN_EQ("$.ab\t\n\rc"s, TokenList{"ab\t\n\rc"s});
  EXPECT_QUOTED_TOKEN_EQ("."s, TokenList{"."s});
  EXPECT_UNQUOTED_TOKEN_INVALID("."s);
  EXPECT_QUOTED_TOKEN_EQ("$"s, TokenList{"$"s});
  EXPECT_TOKEN_EQ("$.$"s, TokenList{"$"s});
  EXPECT_QUOTED_TOKEN_EQ("]"s, TokenList{"]"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("]"s);
  EXPECT_QUOTED_TOKEN_EQ("["s, TokenList{"["s});
  EXPECT_UNQUOTED_TOKEN_INVALID("["s);
  EXPECT_QUOTED_TOKEN_EQ("'"s, TokenList{"'"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("'"s);
  EXPECT_QUOTED_TOKEN_EQ(
      "!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s,
      TokenList{"!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s);
  EXPECT_QUOTED_TOKEN_EQ("a\\\\b\\\""s, TokenList{"a\\b\""s});
  EXPECT_UNQUOTED_TOKEN_INVALID("a\\\\b\\\""s);
  EXPECT_QUOTED_TOKEN_EQ("ab\\\"cd\\\"ef"s, TokenList{"ab\"cd\"ef"s});

  // backslash not followed by valid escape
  EXPECT_TOKEN_INVALID("$[\"a\\ \"]"s);

  // colon in subscript must be quoted
  EXPECT_TOKEN_INVALID("$[foo:bar]"s);

  EXPECT_TOKEN_INVALID("$.store.book[");

  /* test case from ByteDance */
  // 1. Chinese
  EXPECT_TOKEN_EQ("$.所属业务"s, TokenList{"所属业务"s});
  EXPECT_TOKEN_EQ("$.处理进度[0]"s, (TokenList{"处理进度"s, "0"s}));
  EXPECT_TOKEN_EQ("$.处理进度[完成]"s, (TokenList{"处理进度"s, "完成"s}));

  // 2. missing $
  EXPECT_TOKEN_EQ("invisible"s, TokenList{});
  EXPECT_TOKEN_EQ("app"s, TokenList{});
  EXPECT_TOKEN_EQ("shop_id"s, TokenList{});

  // 3. contain $ or #
  EXPECT_TOKEN_EQ("$.#all_deny_reason"s, TokenList{"#all_deny_reason"s});
  EXPECT_TOKEN_EQ(
      "$.feature.$pboc_organ_bert_combined.label_1"s,
      (TokenList{"feature"s, "$pboc_organ_bert_combined"s, "label_1"s}));
  EXPECT_TOKEN_EQ(
      "$.extra_conditions[0].target.$numberLong"s,
      (TokenList{"extra_conditions"s, "0"s, "target"s, "$numberLong"s}));
  EXPECT_TOKEN_EQ(
      "$.skyeye_contact_risk_item_friendly_see.item_friendly_see##operate_type"s,
      (TokenList{
          "skyeye_contact_risk_item_friendly_see"s,
          "item_friendly_see##operate_type"s}));

  // 4. single quote in subscript
  EXPECT_TOKEN_EQ("$['clusters']['0.99']"s, (TokenList{"clusters"s, "0.99"s}));
  EXPECT_TOKEN_EQ("$['0.96']"s, TokenList{"0.96"s});

  // 5. error syntax
  EXPECT_TOKEN_INVALID("$object_info.extra");

  // 6. currently not supported syntax
  EXPECT_TOKEN_INVALID("$..");
  EXPECT_TOKEN_INVALID("$.address..");
}
