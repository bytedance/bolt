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

#pragma once

#include <memory>
#include <sstream>

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <glog/logging.h>

#include <folly/Conv.h>
#include <folly/Exception.h>
#include <folly/Preprocessor.h>
#include "FmtStdFormatters.h"
#include "bolt/common/base/BoltException.h"
namespace bytedance::bolt {
namespace detail {

struct BoltCheckFailArgs {
  const char* file;
  size_t line;
  const char* function;
  const char* expression;
  const char* errorSource;
  const char* errorCode;
  bool isRetriable;
};

struct CompileTimeEmptyString {
  CompileTimeEmptyString() = default;
  constexpr operator const char*() const {
    return "";
  }
  constexpr operator std::string_view() const {
    return {};
  }
  operator std::string() const {
    return {};
  }
};

// boltCheckFail is defined as a separate helper function rather than
// a macro or inline `throw` expression to allow the compiler *not* to
// inline it when it is large. Having an out-of-line error path helps
// otherwise-small functions that call error-checking macros stay
// small and thus stay eligible for inlining.
template <typename Exception, typename StringType>
[[noreturn]] void boltCheckFail(const BoltCheckFailArgs& args, StringType s) {
  static_assert(
      !std::is_same_v<StringType, std::string>,
      "BUG: we should not pass std::string by value to boltCheckFail");
  if constexpr (!std::is_same_v<Exception, BoltUserError>) {
    LOG(ERROR) << "Line: " << args.file << ":" << args.line
               << ", Function:" << args.function
               << ", Expression: " << args.expression << " " << s
               << ", Source: " << args.errorSource
               << ", ErrorCode: " << args.errorCode;
  }

  ++threadNumBoltThrow();
  throw Exception(
      args.file,
      args.line,
      args.function,
      args.expression,
      s,
      args.errorSource,
      args.errorCode,
      args.isRetriable);
}

// BoltCheckFailStringType helps us pass by reference to
// boltCheckFail exactly when the string type is std::string.
template <typename T>
struct BoltCheckFailStringType;

template <>
struct BoltCheckFailStringType<CompileTimeEmptyString> {
  using type = CompileTimeEmptyString;
};

template <>
struct BoltCheckFailStringType<const char*> {
  using type = const char*;
};

template <>
struct BoltCheckFailStringType<std::string> {
  using type = const std::string&;
};

// Declare explicit instantiations of boltCheckFail for the given
// exceptionType. Just like normal function declarations (prototypes),
// this allows the compiler to assume that they are defined elsewhere
// and simply insert a function call for the linker to fix up, rather
// than emitting a definition of these templates into every
// translation unit they are used in.
#define DECLARE_CHECK_FAIL_TEMPLATES(exception_type)                          \
  namespace detail {                                                          \
  extern template void boltCheckFail<exception_type, CompileTimeEmptyString>( \
      const BoltCheckFailArgs& args,                                          \
      CompileTimeEmptyString);                                                \
  extern template void boltCheckFail<exception_type, const char*>(            \
      const BoltCheckFailArgs& args,                                          \
      const char*);                                                           \
  extern template void boltCheckFail<exception_type, const std::string&>(     \
      const BoltCheckFailArgs& args,                                          \
      const std::string&);                                                    \
  } // namespace detail

// Definitions corresponding to DECLARE_CHECK_FAIL_TEMPLATES. Should
// only be used in Exceptions.cpp.
#define DEFINE_CHECK_FAIL_TEMPLATES(exception_type)                    \
  template void boltCheckFail<exception_type, CompileTimeEmptyString>( \
      const BoltCheckFailArgs& args, CompileTimeEmptyString);          \
  template void boltCheckFail<exception_type, const char*>(            \
      const BoltCheckFailArgs& args, const char*);                     \
  template void boltCheckFail<exception_type, const std::string&>(     \
      const BoltCheckFailArgs& args, const std::string&);

// When there is no message passed, we can statically detect this case
// and avoid passing even a single unnecessary argument pointer,
// minimizing size and thus maximizing eligibility for inlining.
inline CompileTimeEmptyString errorMessage() {
  return {};
}

inline const char* errorMessage(const char* s) {
  return s;
}

inline std::string errorMessage(const std::string& str) {
  return str;
}

template <typename... Args>
std::string errorMessage(fmt::string_view fmt, const Args&... args) {
  return fmt::vformat(fmt, fmt::make_format_args(args...));
}

} // namespace detail

#define _BOLT_THROW_IMPL(                                                \
    exception, exprStr, errorSource, errorCode, isRetriable, ...)        \
  {                                                                      \
    /* GCC 9.2.1 doesn't accept this code with constexpr. */             \
    static const ::bytedance::bolt::detail::BoltCheckFailArgs            \
        boltCheckFailArgs = {                                            \
            __FILE__,                                                    \
            __LINE__,                                                    \
            __FUNCTION__,                                                \
            exprStr,                                                     \
            errorSource,                                                 \
            errorCode,                                                   \
            isRetriable};                                                \
    auto message = ::bytedance::bolt::detail::errorMessage(__VA_ARGS__); \
    ::bytedance::bolt::detail::boltCheckFail<                            \
        exception,                                                       \
        typename ::bytedance::bolt::detail::BoltCheckFailStringType<     \
            decltype(message)>::type>(boltCheckFailArgs, message);       \
  }

#define _BOLT_CHECK_AND_THROW_IMPL(                                            \
    expr, exprStr, exception, errorSource, errorCode, isRetriable, ...)        \
  if (UNLIKELY(!(expr))) {                                                     \
    _BOLT_THROW_IMPL(                                                          \
        exception, exprStr, errorSource, errorCode, isRetriable, __VA_ARGS__); \
  }

#define _BOLT_THROW(exception, ...) \
  _BOLT_THROW_IMPL(exception, "", ##__VA_ARGS__)

DECLARE_CHECK_FAIL_TEMPLATES(::bytedance::bolt::BoltRuntimeError);

#define _BOLT_CHECK_IMPL(expr, exprStr, ...)                        \
  _BOLT_CHECK_AND_THROW_IMPL(                                       \
      expr,                                                         \
      exprStr,                                                      \
      ::bytedance::bolt::BoltRuntimeError,                          \
      ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(), \
      ::bytedance::bolt::error_code::kInvalidState.c_str(),         \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

// If the caller passes a custom message (4 *or more* arguments), we
// have to construct a format string from ours ("({} vs. {})") plus
// theirs by adding a space and shuffling arguments. If they don't (exactly 3
// arguments), we can just pass our own format string and arguments straight
// through.

#define _BOLT_CHECK_OP_WITH_USER_FMT_HELPER(    \
    implmacro, expr1, expr2, op, user_fmt, ...) \
  implmacro(                                    \
      (expr1)op(expr2),                         \
      #expr1 " " #op " " #expr2,                \
      "({} vs. {}) " user_fmt,                  \
      expr1,                                    \
      expr2,                                    \
      ##__VA_ARGS__)

#define _BOLT_CHECK_OP_HELPER(implmacro, expr1, expr2, op, ...) \
  if constexpr (FOLLY_PP_DETAIL_NARGS(__VA_ARGS__) > 0) {       \
    _BOLT_CHECK_OP_WITH_USER_FMT_HELPER(                        \
        implmacro, expr1, expr2, op, __VA_ARGS__);              \
  } else {                                                      \
    implmacro(                                                  \
        (expr1)op(expr2),                                       \
        #expr1 " " #op " " #expr2,                              \
        "({} vs. {})",                                          \
        expr1,                                                  \
        expr2);                                                 \
  }

#define _BOLT_CHECK_OP(expr1, expr2, op, ...) \
  _BOLT_CHECK_OP_HELPER(_BOLT_CHECK_IMPL, expr1, expr2, op, ##__VA_ARGS__)

#define _BOLT_USER_CHECK_IMPL(expr, exprStr, ...)                \
  _BOLT_CHECK_AND_THROW_IMPL(                                    \
      expr,                                                      \
      exprStr,                                                   \
      ::bytedance::bolt::BoltUserError,                          \
      ::bytedance::bolt::error_source::kErrorSourceUser.c_str(), \
      ::bytedance::bolt::error_code::kInvalidArgument.c_str(),   \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define _BOLT_USER_CHECK_OP(expr1, expr2, op, ...) \
  _BOLT_CHECK_OP_HELPER(_BOLT_USER_CHECK_IMPL, expr1, expr2, op, ##__VA_ARGS__)

// For all below macros, an additional message can be passed using a
// format string and arguments, as with `fmt::format`.
#define BOLT_CHECK(expr, ...) _BOLT_CHECK_IMPL(expr, #expr, ##__VA_ARGS__)
#define BOLT_CHECK_GT(e1, e2, ...) _BOLT_CHECK_OP(e1, e2, >, ##__VA_ARGS__)
#define BOLT_CHECK_GE(e1, e2, ...) _BOLT_CHECK_OP(e1, e2, >=, ##__VA_ARGS__)
#define BOLT_CHECK_LT(e1, e2, ...) _BOLT_CHECK_OP(e1, e2, <, ##__VA_ARGS__)
#define BOLT_CHECK_LE(e1, e2, ...) _BOLT_CHECK_OP(e1, e2, <=, ##__VA_ARGS__)
#define BOLT_CHECK_EQ(e1, e2, ...) _BOLT_CHECK_OP(e1, e2, ==, ##__VA_ARGS__)
#define BOLT_CHECK_NE(e1, e2, ...) _BOLT_CHECK_OP(e1, e2, !=, ##__VA_ARGS__)
#define BOLT_CHECK_NULL(e, ...) BOLT_CHECK(e == nullptr, ##__VA_ARGS__)
#define BOLT_CHECK_NOT_NULL(e, ...) BOLT_CHECK(e != nullptr, ##__VA_ARGS__)

#define BOLT_CHECK_OK(expr)                          \
  do {                                               \
    ::bytedance::bolt::Status _s = (expr);           \
    _BOLT_CHECK_IMPL(_s.ok(), #expr, _s.toString()); \
  } while (false)

#define BOLT_UNSUPPORTED(...)                                    \
  _BOLT_THROW(                                                   \
      ::bytedance::bolt::BoltUserError,                          \
      ::bytedance::bolt::error_source::kErrorSourceUser.c_str(), \
      ::bytedance::bolt::error_code::kUnsupported.c_str(),       \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define BOLT_ARITHMETIC_ERROR(...)                               \
  _BOLT_THROW(                                                   \
      ::bytedance::bolt::BoltUserError,                          \
      ::bytedance::bolt::error_source::kErrorSourceUser.c_str(), \
      ::bytedance::bolt::error_code::kArithmeticError.c_str(),   \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define BOLT_SCHEMA_MISMATCH_ERROR(...)                          \
  _BOLT_THROW(                                                   \
      ::bytedance::bolt::BoltUserError,                          \
      ::bytedance::bolt::error_source::kErrorSourceUser.c_str(), \
      ::bytedance::bolt::error_code::kSchemaMismatch.c_str(),    \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define BOLT_FILE_NOT_FOUND_ERROR(...)                              \
  _BOLT_THROW(                                                      \
      ::bytedance::bolt::BoltRuntimeError,                          \
      ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(), \
      ::bytedance::bolt::error_code::kFileNotFound.c_str(),         \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

#define BOLT_UNREACHABLE(...)                                       \
  _BOLT_THROW(                                                      \
      ::bytedance::bolt::BoltRuntimeError,                          \
      ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(), \
      ::bytedance::bolt::error_code::kUnreachableCode.c_str(),      \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

#ifndef NDEBUG
#define BOLT_DCHECK(expr, ...) BOLT_CHECK(expr, ##__VA_ARGS__)
#define BOLT_DCHECK_GT(e1, e2, ...) BOLT_CHECK_GT(e1, e2, ##__VA_ARGS__)
#define BOLT_DCHECK_GE(e1, e2, ...) BOLT_CHECK_GE(e1, e2, ##__VA_ARGS__)
#define BOLT_DCHECK_LT(e1, e2, ...) BOLT_CHECK_LT(e1, e2, ##__VA_ARGS__)
#define BOLT_DCHECK_LE(e1, e2, ...) BOLT_CHECK_LE(e1, e2, ##__VA_ARGS__)
#define BOLT_DCHECK_EQ(e1, e2, ...) BOLT_CHECK_EQ(e1, e2, ##__VA_ARGS__)
#define BOLT_DCHECK_NE(e1, e2, ...) BOLT_CHECK_NE(e1, e2, ##__VA_ARGS__)
#define BOLT_DCHECK_NULL(e, ...) BOLT_CHECK_NULL(e, ##__VA_ARGS__)
#define BOLT_DCHECK_NOT_NULL(e, ...) BOLT_CHECK_NOT_NULL(e, ##__VA_ARGS__)
#else
#define BOLT_DCHECK(expr, ...) BOLT_CHECK(true)
#define BOLT_DCHECK_GT(e1, e2, ...) BOLT_CHECK(true)
#define BOLT_DCHECK_GE(e1, e2, ...) BOLT_CHECK(true)
#define BOLT_DCHECK_LT(e1, e2, ...) BOLT_CHECK(true)
#define BOLT_DCHECK_LE(e1, e2, ...) BOLT_CHECK(true)
#define BOLT_DCHECK_EQ(e1, e2, ...) BOLT_CHECK(true)
#define BOLT_DCHECK_NE(e1, e2, ...) BOLT_CHECK(true)
#define BOLT_DCHECK_NOT_NULL(e, ...) BOLT_CHECK(true)
#endif

#define BOLT_FAIL(...)                                              \
  _BOLT_THROW(                                                      \
      ::bytedance::bolt::BoltRuntimeError,                          \
      ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(), \
      ::bytedance::bolt::error_code::kInvalidState.c_str(),         \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

DECLARE_CHECK_FAIL_TEMPLATES(::bytedance::bolt::BoltUserError);

// For all below macros, an additional message can be passed using a
// format string and arguments, as with `fmt::format`.
#define BOLT_USER_CHECK(expr, ...) \
  _BOLT_USER_CHECK_IMPL(expr, #expr, ##__VA_ARGS__)
#define BOLT_USER_CHECK_GT(e1, e2, ...) \
  _BOLT_USER_CHECK_OP(e1, e2, >, ##__VA_ARGS__)
#define BOLT_USER_CHECK_GE(e1, e2, ...) \
  _BOLT_USER_CHECK_OP(e1, e2, >=, ##__VA_ARGS__)
#define BOLT_USER_CHECK_LT(e1, e2, ...) \
  _BOLT_USER_CHECK_OP(e1, e2, <, ##__VA_ARGS__)
#define BOLT_USER_CHECK_LE(e1, e2, ...) \
  _BOLT_USER_CHECK_OP(e1, e2, <=, ##__VA_ARGS__)
#define BOLT_USER_CHECK_EQ(e1, e2, ...) \
  _BOLT_USER_CHECK_OP(e1, e2, ==, ##__VA_ARGS__)
#define BOLT_USER_CHECK_NE(e1, e2, ...) \
  _BOLT_USER_CHECK_OP(e1, e2, !=, ##__VA_ARGS__)
#define BOLT_USER_CHECK_NULL(e, ...) \
  BOLT_USER_CHECK(e == nullptr, ##__VA_ARGS__)
#define BOLT_USER_CHECK_NOT_NULL(e, ...) \
  BOLT_USER_CHECK(e != nullptr, ##__VA_ARGS__)

#ifndef NDEBUG
#define BOLT_USER_DCHECK(expr, ...) BOLT_USER_CHECK(expr, ##__VA_ARGS__)
#define BOLT_USER_DCHECK_GT(e1, e2, ...) \
  BOLT_USER_CHECK_GT(e1, e2, ##__VA_ARGS__)
#define BOLT_USER_DCHECK_GE(e1, e2, ...) \
  BOLT_USER_CHECK_GE(e1, e2, ##__VA_ARGS__)
#define BOLT_USER_DCHECK_LT(e1, e2, ...) \
  BOLT_USER_CHECK_LT(e1, e2, ##__VA_ARGS__)
#define BOLT_USER_DCHECK_LE(e1, e2, ...) \
  BOLT_USER_CHECK_LE(e1, e2, ##__VA_ARGS__)
#define BOLT_USER_DCHECK_EQ(e1, e2, ...) \
  BOLT_USER_CHECK_EQ(e1, e2, ##__VA_ARGS__)
#define BOLT_USER_DCHECK_NE(e1, e2, ...) \
  BOLT_USER_CHECK_NE(e1, e2, ##__VA_ARGS__)
#define BOLT_USER_DCHECK_NOT_NULL(e, ...) \
  BOLT_USER_CHECK_NOT_NULL(e, ##__VA_ARGS__)
#define BOLT_USER_DCHECK_NULL(e, ...) BOLT_USER_CHECK_NULL(e, ##__VA_ARGS__)
#else
#define BOLT_USER_DCHECK(expr, ...) BOLT_USER_CHECK(true)
#define BOLT_USER_DCHECK_GT(e1, e2, ...) BOLT_USER_CHECK(true)
#define BOLT_USER_DCHECK_GE(e1, e2, ...) BOLT_USER_CHECK(true)
#define BOLT_USER_DCHECK_LT(e1, e2, ...) BOLT_USER_CHECK(true)
#define BOLT_USER_DCHECK_LE(e1, e2, ...) BOLT_USER_CHECK(true)
#define BOLT_USER_DCHECK_EQ(e1, e2, ...) BOLT_USER_CHECK(true)
#define BOLT_USER_DCHECK_NE(e1, e2, ...) BOLT_USER_CHECK(true)
#define BOLT_USER_DCHECK_NULL(e, ...) BOLT_USER_CHECK(true)
#define BOLT_USER_DCHECK_NOT_NULL(e, ...) BOLT_USER_CHECK(true)
#endif

#define BOLT_USER_FAIL(...)                                      \
  _BOLT_THROW(                                                   \
      ::bytedance::bolt::BoltUserError,                          \
      ::bytedance::bolt::error_source::kErrorSourceUser.c_str(), \
      ::bytedance::bolt::error_code::kInvalidArgument.c_str(),   \
      /* isRetriable */ false,                                   \
      ##__VA_ARGS__)

#define BOLT_NYI(...)                                               \
  _BOLT_THROW(                                                      \
      ::bytedance::bolt::BoltRuntimeError,                          \
      ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(), \
      ::bytedance::bolt::error_code::kNotImplemented.c_str(),       \
      /* isRetriable */ false,                                      \
      ##__VA_ARGS__)

} // namespace bytedance::bolt
