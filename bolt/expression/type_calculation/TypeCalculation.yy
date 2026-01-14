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
#include <FlexLexer.h>
#include <bolt/common/base/Exceptions.h>
%}
%require "3.0.4"
%language "C++"

%define api.parser.class {Parser}
%define api.namespace {bytedance::bolt::expression::calculate}
%define api.value.type variant
%parse-param {Scanner* scanner}
%define parse.error verbose

%code requires
{
    namespace bytedance::bolt::expression::calculate {
        class Scanner;
    } // namespace bytedance::bolt::expression::calculate
} // %code requires

%code
{
    #include "bolt/expression/type_calculation/Scanner.h"
    #define yylex(x) scanner->lex(x)
}

%token               LPAREN RPAREN COMMA MIN MAX TERNARY COLON
%token <long long>   INT
%token <std::string> VAR
%token YYEOF         0

%nterm <long long>  iexp

%nonassoc           ASSIGN
%right              TERNARY COLON
%left               EQ NEQ
%left               LT LTE GT GTE
%left               PLUS MINUS
%left               MULTIPLY DIVIDE MODULO
%precedence         UMINUS

%%

calc    : VAR ASSIGN iexp           { scanner->setValue($1, $3); }
        | error                     { yyerrok; }
        ;

iexp    : INT                       { $$ = $1; }
        | iexp PLUS iexp            { $$ = $1 + $3; }
        | iexp MINUS iexp           { $$ = $1 - $3; }
        | iexp MULTIPLY iexp        { $$ = $1 * $3; }
        | iexp DIVIDE iexp          { $$ = $1 / $3; }
        | iexp MODULO iexp          { $$ = $1 % $3; }
        | MINUS iexp %prec UMINUS   { $$ = -$2; }
        | LPAREN iexp RPAREN        { $$ = $2; }
        | MAX LPAREN iexp COMMA iexp RPAREN { $$ = std::max($3, $5); }
        | MIN LPAREN iexp COMMA iexp RPAREN { $$ = std::min($3, $5); }
        | iexp LT iexp              { $$ = $1 < $3; }
        | iexp LTE iexp             { $$ = $1 <= $3; }
        | iexp GT iexp              { $$ = $1 > $3; }
        | iexp GTE iexp             { $$ = $1 >= $3; }
        | iexp EQ iexp              { $$ = $1 == $3; }
        | iexp NEQ iexp             { $$ = $1 != $3; }
        | iexp TERNARY iexp COLON iexp { $$ = $1 ? $3 : $5; }
        | VAR                       { $$ = scanner->getValue($1); }
        ;

%%

void bytedance::bolt::expression::calculate::Parser::error(const std::string& msg) {
    BOLT_FAIL(msg);
}
