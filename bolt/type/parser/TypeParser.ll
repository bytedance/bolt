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

%{
#include <vector>
#include <memory>

#include "bolt/type/parser/TypeParser.yy.h"  // @manual
#include "bolt/type/parser/Scanner.h"
#define YY_DECL int bytedance::bolt::type::Scanner::lex(bytedance::bolt::type::Parser::semantic_type *yylval)
%}

%option c++ noyywrap noyylineno nodefault caseless

WORD              ([[:alpha:][:alnum:]_]*)
QUOTED_ID         (['"'][[:alnum:][:space:]_]*['"'])
NUMBER            ([[:digit:]]+)

%%

"("                return Parser::token::LPAREN;
")"                return Parser::token::RPAREN;
","                return Parser::token::COMMA;
(ARRAY)            return Parser::token::ARRAY;
(MAP)              return Parser::token::MAP;
(FUNCTION)         return Parser::token::FUNCTION;
(DECIMAL)          return Parser::token::DECIMAL;
(ROW)              return Parser::token::ROW;
VARCHAR|VARBINARY  yylval->build<std::string>(YYText()); return Parser::token::VARIABLE;
{NUMBER}           yylval->build<long long>(folly::to<int>(YYText())); return Parser::token::NUMBER;
{WORD}             yylval->build<std::string>(YYText()); return Parser::token::WORD;
{QUOTED_ID}        yylval->build<std::string>(YYText()); return Parser::token::QUOTED_ID;
<<EOF>>            return Parser::token::YYEOF;
(.|\n)             /* no action on unmatched input */

%%

int yyFlexLexer::yylex() {
    throw std::runtime_error("Bad call to yyFlexLexer::yylex()");
}

#include "bolt/type/parser/TypeParser.h"

bytedance::bolt::TypePtr bytedance::bolt::parseType(const std::string& typeText)
 {
    std::istringstream is(typeText);
    std::ostringstream os;
    bytedance::bolt::TypePtr type;
    bytedance::bolt::type::Scanner scanner{is, os, type, typeText};
    bytedance::bolt::type::Parser parser{ &scanner };
    parser.parse();
    BOLT_CHECK(type, "Failed to parse type [{}]", typeText);
    return type;
}
