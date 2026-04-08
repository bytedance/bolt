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

#include "bolt/core/Metaprogramming.h"
#include "bolt/type/SimpleFunctionApi.h"
#include "bolt/type/Type.h"

#define BOLT_DEFINE_FUNCTION_TYPES(__Bolt_ExecParams)                  \
  template <typename TArgs>                                            \
  using arg_type =                                                     \
      typename __Bolt_ExecParams::template resolver<TArgs>::in_type;   \
                                                                       \
  template <typename TArgs>                                            \
  using out_type =                                                     \
      typename __Bolt_ExecParams::template resolver<TArgs>::out_type;  \
                                                                       \
  template <typename TArgs>                                            \
  using opt_arg_type = std::optional<                                  \
      typename __Bolt_ExecParams::template resolver<TArgs>::in_type>;  \
                                                                       \
  template <typename TArgs>                                            \
  using opt_out_type = std::optional<                                  \
      typename __Bolt_ExecParams::template resolver<TArgs>::out_type>; \
                                                                       \
  DECLARE_CONDITIONAL_TYPE_NAME(                                       \
      null_free_in_type_resolver, null_free_in_type, in_type);         \
  template <typename TArgs>                                            \
  using null_free_arg_type =                                           \
      typename null_free_in_type_resolver::template resolve<           \
          typename __Bolt_ExecParams::template resolver<TArgs>>::type; \
                                                                       \
  template <typename TKey, typename TVal>                              \
  using MapVal = arg_type<::bytedance::bolt::Map<TKey, TVal>>;         \
  template <typename TElement>                                         \
  using ArrayVal = arg_type<::bytedance::bolt::Array<TElement>>;       \
  using VarcharVal = arg_type<::bytedance::bolt::Varchar>;             \
  using VarbinaryVal = arg_type<::bytedance::bolt::Varbinary>;         \
  template <typename... TArgss>                                        \
  using RowVal = arg_type<::bytedance::bolt::Row<TArgss...>>;          \
  template <typename TKey, typename TVal>                              \
  using MapWriter = out_type<::bytedance::bolt::Map<TKey, TVal>>;      \
  template <typename TElement>                                         \
  using ArrayWriter = out_type<::bytedance::bolt::Array<TElement>>;    \
  using VarcharWriter = out_type<::bytedance::bolt::Varchar>;          \
  using VarbinaryWriter = out_type<::bytedance::bolt::Varbinary>;      \
  template <typename... TArgss>                                        \
  using RowWriter = out_type<::bytedance::bolt::Row<TArgss...>>;

#define BOLT_UDF_BEGIN(Name)                        \
  struct udf_##Name {                               \
    static constexpr auto name = #Name;             \
    template <typename __Bolt_ExecParams>           \
    struct udf {                                    \
      BOLT_DEFINE_FUNCTION_TYPES(__Bolt_ExecParams) \
      static constexpr auto name = #Name;

#define BOLT_UDF_END() \
  }                    \
  ;                    \
  }                    \
  ;
