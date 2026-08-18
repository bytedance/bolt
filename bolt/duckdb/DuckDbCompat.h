/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>

namespace duckdb {
class QueryNode;
class SelectStatement;
struct CommonTableExpressionInfo;
} // namespace duckdb

namespace std {
// DuckDB parser headers contain unique_ptr cycles that clang16/libstdc++13
// instantiates before the pointee types are complete.
template <>
struct default_delete<duckdb::QueryNode> {
  constexpr default_delete() noexcept = default;
  void operator()(duckdb::QueryNode* ptr) const;
};

template <>
struct default_delete<duckdb::SelectStatement> {
  constexpr default_delete() noexcept = default;
  void operator()(duckdb::SelectStatement* ptr) const;
};

template <>
struct default_delete<duckdb::CommonTableExpressionInfo> {
  constexpr default_delete() noexcept = default;
  void operator()(duckdb::CommonTableExpressionInfo* ptr) const;
};
} // namespace std

#include <duckdb.hpp> // @manual

namespace std {
inline void default_delete<duckdb::QueryNode>::operator()(
    duckdb::QueryNode* ptr) const {
  delete ptr;
}

inline void default_delete<duckdb::SelectStatement>::operator()(
    duckdb::SelectStatement* ptr) const {
  delete ptr;
}

inline void default_delete<duckdb::CommonTableExpressionInfo>::operator()(
    duckdb::CommonTableExpressionInfo* ptr) const {
  delete ptr;
}
} // namespace std
