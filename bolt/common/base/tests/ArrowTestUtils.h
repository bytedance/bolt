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

#pragma once

#include <utility>

#include <gtest/gtest.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/string_builder.h"

namespace bytedance::bolt::test::detail {

inline ::arrow::Status arrowStatus(const ::arrow::Status& status) {
  return status;
}

template <typename T>
::arrow::Status arrowStatus(const ::arrow::Result<T>& result) {
  return result.status();
}

} // namespace bytedance::bolt::test::detail

// Keep these names aligned with the Arrow-derived tests while using only
// Arrow's public runtime APIs.
#define ASSERT_OK(expression)                                          \
  for (const auto _boltArrowStatus =                                   \
           ::bytedance::bolt::test::detail::arrowStatus((expression)); \
       !_boltArrowStatus.ok();)                                        \
  FAIL() << "'" #expression "' failed with " << _boltArrowStatus.ToString()

#define BOLT_ARROW_EXPECT_OK(expression)                                    \
  do {                                                                      \
    auto&& _boltArrowResult = (expression);                                 \
    const auto _boltArrowStatus =                                           \
        ::bytedance::bolt::test::detail::arrowStatus(_boltArrowResult);     \
    EXPECT_TRUE(_boltArrowStatus.ok())                                      \
        << "'" #expression "' failed with " << _boltArrowStatus.ToString(); \
  } while (false)

#define BOLT_ARROW_CONCAT_IMPL(x, y) x##y
#define BOLT_ARROW_CONCAT(x, y) BOLT_ARROW_CONCAT_IMPL(x, y)

#define BOLT_ARROW_ASSIGN_OR_HANDLE_ERROR_IMPL( \
    handleError, statusName, lhs, expression)   \
  auto&& statusName = (expression);             \
  handleError(statusName.status());             \
  lhs = std::move(statusName).ValueOrDie()

#define ASSERT_OK_AND_ASSIGN(lhs, expression)           \
  BOLT_ARROW_ASSIGN_OR_HANDLE_ERROR_IMPL(               \
      ASSERT_OK,                                        \
      BOLT_ARROW_CONCAT(_boltArrowResult, __COUNTER__), \
      lhs,                                              \
      expression)

#define EXPECT_OK_AND_ASSIGN(lhs, expression)           \
  BOLT_ARROW_ASSIGN_OR_HANDLE_ERROR_IMPL(               \
      BOLT_ARROW_EXPECT_OK,                             \
      BOLT_ARROW_CONCAT(_boltArrowResult, __COUNTER__), \
      lhs,                                              \
      expression)

#define ARROW_SCOPED_TRACE(...) \
  SCOPED_TRACE(::arrow::util::StringBuilder(__VA_ARGS__))
