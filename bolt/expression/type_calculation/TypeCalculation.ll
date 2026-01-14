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
#include "bolt/expression/type_calculation/TypeCalculation.yy.h"  // @manual
#include "bolt/expression/type_calculation/Scanner.h"
#define YY_DECL int bytedance::bolt::expression::calculate::Scanner::lex(bytedance::bolt::expression::calculate::Parser::semantic_type *yylval)
%}

%option c++ noyywrap noyylineno nodefault

integer         ([[:digit:]]+)
var             ([[:alpha:]][[:alnum:]_]*)

%%

"+"             return Parser::token::PLUS;
"-"             return Parser::token::MINUS;
"*"             return Parser::token::MULTIPLY;
"/"             return Parser::token::DIVIDE;
"%"             return Parser::token::MODULO;
"("             return Parser::token::LPAREN;
")"             return Parser::token::RPAREN;
","             return Parser::token::COMMA;
"="             return Parser::token::ASSIGN;
"min"           return Parser::token::MIN;
"max"           return Parser::token::MAX;
"<"             return Parser::token::LT;
"<="            return Parser::token::LTE;
">"             return Parser::token::GT;
">="            return Parser::token::GTE;
"=="            return Parser::token::EQ;
"!="            return Parser::token::NEQ;
"?"             return Parser::token::TERNARY;
":"             return Parser::token::COLON;
{integer}       yylval->build<long long>(strtoll(YYText(), nullptr, 10)); return Parser::token::INT;
{var}           yylval->build<std::string>(YYText()); return Parser::token::VAR;
<<EOF>>         return Parser::token::YYEOF;
(.|\n)          /* no action on unmatched input */

%%

int yyFlexLexer::yylex() {
    throw std::runtime_error("Bad call to yyFlexLexer::yylex()");
}

#include "bolt/expression/type_calculation/TypeCalculation.h"

void bytedance::bolt::expression::calculation::evaluate(const std::string& calculation, std::unordered_map<std::string, int>& variables) {
    std::istringstream is(calculation);
    bytedance::bolt::expression::calculate::Scanner scanner{ is, std::cerr, variables};
    bytedance::bolt::expression::calculate::Parser parser{ &scanner };
    parser.parse();
}
