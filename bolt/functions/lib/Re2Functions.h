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

#include <memory>
#include <vector>

#include <re2/re2.h>
#include <re2/stringpiece.h>

#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/Udf.h"
#include "bolt/vector/BaseVector.h"
namespace bytedance::bolt::functions {

/// Representation of different kinds of patterns.
enum class PatternKind {
  /// Pattern containing wildcard character '_' only, such as _, __, ____.
  kExactlyN,
  /// Pattern containing wildcard characters ('_' or '%') only with at least one
  /// '%', such as ___%, _%__.
  kAtLeastN,
  /// Pattern with no wildcard characters, such as 'presto', 'foo'.
  kFixed,
  /// Fixed pattern followed by one or more '%', such as 'hello%', 'foo%%%%'.
  kPrefix,
  /// Fixed pattern preceded by one or more '%', such as '%foo', '%%%hello'.
  kSuffix,
  /// Patterns matching '%{c0}%', such as '%foo%%', '%%%hello%'.
  kSubstring,
  /// Patterns which do not fit any of the above types, such as 'hello_world',
  /// '_presto%'.
  kGeneric,
};

struct PatternMetadata {
  PatternKind patternKind;
  // Contains the length of the unescaped fixed pattern for patterns of kind
  // kFixed, kPrefix, kSuffix and kSubstring. Contains the count of wildcard
  // character '_' for patterns of kind kExactlyN and kAtLeastN. Contains 0
  // otherwise.
  size_t length;
  // Contains the unescaped fixed pattern in patterns of kind kFixed, kPrefix,
  // kSuffix and kSubstring.
  std::string fixedPattern = "";
};
inline const int kMaxCompiledRegexes = 20;

/// The functions in this file use RE2 as the regex engine. RE2 is fast, but
/// supports only a subset of PCRE syntax and in particular does not support
/// backtracking and associated features (e.g. backreferences).
/// See https://github.com/google/re2/wiki/Syntax for more information.

/// re2Match(string, pattern) → bool
///
/// Returns whether str matches the regex pattern.  pattern will be parsed using
/// RE2 pattern syntax, a subset of PCRE. If the pattern is invalid, throws an
/// exception.
std::shared_ptr<exec::VectorFunction> makeRe2Match(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2MatchSignatures();

/// re2Search(string, pattern) → bool
///
/// Returns whether str has a substr that matches the regex pattern.  pattern
/// will be parsed using RE2 pattern syntax, a subset of PCRE. If the pattern is
/// invalid, throws an exception.
std::shared_ptr<exec::VectorFunction> makeRe2Search(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2SearchSignatures();

/// re2Extract(string, pattern, group_id) → string
/// re2Extract(string, pattern) → string
///
/// If string has a substring that matches the given pattern, returns the
/// substring matching the given group in the pattern. pattern will be parsed
/// using the RE2 pattern syntax, a subset of PCRE. Groups are 1-indexed.
/// Providing zero as the group_id extracts and returns the entire match; this
/// is more efficient than extracting a subgroup. Extracting the first subgroup
/// is more efficient than extracting larger indexes; use non-capturing
/// subgroups (?:...) if the pattern includes groups that don't need to be
/// captured.
///
/// If the pattern is invalid or the group id is out of range, throws an
/// exception. If the pattern does not match, returns null.
///
/// If group_id parameter is not specified, extracts and returns the entire
/// match.
std::shared_ptr<exec::VectorFunction> makeRe2Extract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config,
    const bool emptyNoMatch);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2ExtractSignatures();

/// Return the pair {pattern kind, length of the fixed pattern} for fixed,
/// prefix, and suffix patterns. Return the pair {pattern kind, number of '_'
/// characters} for patterns with wildcard characters only. Return
/// {kGenericPattern, 0} for generic patterns).
PatternMetadata determinePatternKind(
    std::string_view pattern,
    std::optional<char> escapeChar);

std::shared_ptr<exec::VectorFunction> makeLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> likeSignatures();

/// re2ExtractAll(string, pattern, group_id) → array<string>
/// re2ExtractAll(string, pattern) → array<string>
///
/// If string has a substring that matches the given pattern, returns ALL of the
/// substrings matching the given group in the pattern. pattern will be parsed
/// using the RE2 pattern syntax, a subset of PCRE. Groups are 1-indexed.
/// Providing zero as the group_id extracts and returns the entire match; this
/// is more efficient than extracting a subgroup. Extracting the first subgroup
/// is more efficient than extracting larger indexes; use non-capturing
/// subgroups (?:...) if the pattern includes groups that don't need to be
/// captured.
///
/// If the pattern is invalid or the group id is out of range, throws an
/// exception. If the pattern does not match, returns null.
///
/// If group_id parameter is not specified, extracts and returns the entire
/// match.
std::shared_ptr<exec::VectorFunction> makeRe2ExtractAll(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2ExtractAllSignatures();

/// regexp_replace(string, pattern, replacement) -> string
/// regexp_replace(string, pattern) -> string
///
/// If string has substrings that match the given pattern, return a new string
/// that has all the matched substrings replaced with the given replacement
/// sequence or removed if no replacement sequence is provided. pattern will
/// be parsed using the RE2 pattern syntax, a subset of PCRE. If pattern is
/// invalid for RE2, this function throws an exception. replacement is a string
/// that may contain references to the named or numbered capturing groups in the
/// pattern. If referenced capturing group names in replacement are invalid for
/// RE2, this function throws an exception.
template <
    typename T,
    std::string (*prepareRegexpPattern)(const StringView&),
    std::string (*prepareRegexpReplacement)(const RE2&, const StringView&)>
struct Re2RegexpReplace {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  std::string processedReplacement_;
  std::string result_;
  std::optional<RE2> re_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*string*/,
      const arg_type<Varchar>* pattern,
      const arg_type<Varchar>* replacement,
      const arg_type<int32_t>* /*pos*/) {
    BOLT_USER_CHECK(
        pattern != nullptr, "Pattern of regexp_replace must be constant.");
    BOLT_USER_CHECK(
        replacement != nullptr,
        "Replacement sequence of regexp_replace must be constant.");

    auto processedPattern = prepareRegexpPattern(*pattern);

    re_.emplace(processedPattern, RE2::Quiet);
    if (UNLIKELY(!re_->ok())) {
      BOLT_USER_FAIL(
          "Invalid regular expression {}: {}.", processedPattern, re_->error());
    }

    processedReplacement_ = prepareRegexpReplacement(*re_, *replacement);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* string,
      const arg_type<Varchar>* pattern) {
    int32_t pos;
    StringView emptyReplacement;
    initialize(inputTypes, config, string, pattern, &emptyReplacement, &pos);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* string,
      const arg_type<Varchar>* pattern,
      const arg_type<Varchar>* replacement) {
    StringView emptyReplacement;
    initialize(inputTypes, config, string, pattern, &emptyReplacement);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& out,
      const arg_type<Varchar>& string,
      const arg_type<Varchar>& /*pattern*/,
      const arg_type<Varchar>& /*replacement*/ = StringView{},
      const arg_type<int32_t>& pos = 1) {
    BOLT_USER_CHECK(
        pos >= 1, "regexp_replace function pos parameter should start from 1");
    if (UNLIKELY(string.empty())) {
      return true;
    }
    if (UNLIKELY(pos > string.size())) {
      out.resize(string.size());
      std::copy(string.begin(), string.end(), out.data());
      return true;
    }

    int32_t idx = pos - 1;

    result_.assign(string.data() + idx, string.size() - idx);
    RE2::GlobalReplace(&result_, *re_, processedReplacement_);

    out.resize(idx + result_.size());
    std::copy(string.begin(), string.begin() + idx, out.data());
    std::copy(result_.begin(), result_.end(), out.data() + idx);

    return true;
  }
};

template <typename T, std::string (*prepareRegexpPattern)(const StringView&)>
struct Re2RegexpSplit {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  std::optional<RE2> re_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*string*/,
      const arg_type<Varchar>* pattern) {
    BOLT_USER_CHECK(
        pattern != nullptr, "Pattern of regexp_split must be constant.");

    auto processedPattern = prepareRegexpPattern(*pattern);

    re_.emplace(processedPattern, RE2::Quiet);
    if (UNLIKELY(!re_->ok())) {
      BOLT_USER_FAIL(
          "Invalid regular expression {}: {}.", processedPattern, re_->error());
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<bolt::Array<Varchar>>& out,
      const arg_type<Varchar>& string,
      const arg_type<Varchar>& /*pattern*/) {
    re2::StringPiece input(string.data(), string.size());
    re2::StringPiece match;
    size_t lastEnd = 0, offset = 0;
    while (re_->Match(
        input, offset, input.length(), RE2::Anchor::UNANCHORED, &match, 1)) {
      size_t length = match.data() - input.data() - lastEnd;
      out.add_item().copy_from(StringView(input.data() + lastEnd, length));

      lastEnd = match.data() - input.data() + match.length();
      offset = offset == lastEnd ? offset + 1 : lastEnd;
    }
    out.add_item().copy_from(
        StringView(input.data() + lastEnd, input.length() - lastEnd));
    return true;
  }
};

} // namespace bytedance::bolt::functions

template <>
struct fmt::formatter<bytedance::bolt::functions::PatternKind>
    : formatter<int> {
  auto format(bytedance::bolt::functions::PatternKind s, format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
