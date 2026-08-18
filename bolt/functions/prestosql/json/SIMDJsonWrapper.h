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

// clang16 cannot parse parts of libstdc++13's C++23 <ranges>. simdjson only
// uses this macro to enable optional std::ranges view adapters.
#include "bolt/common/base/Portability.h"

#if defined(BOLT_CLANG_LIBSTDCXX_INT128_COMPAT)
#ifndef SIMDJSON_DISABLE_RANGES
#define SIMDJSON_DISABLE_RANGES
#define BOLT_SIMDJSON_UNDEFINE_DISABLE_RANGES
#endif

#if defined(__cpp_lib_ranges)
#define BOLT_SIMDJSON_RESTORE_CPP_LIB_RANGES
#pragma push_macro("__cpp_lib_ranges")
#undef __cpp_lib_ranges
#endif
#endif

#if __has_include("simdjson/singleheader/simdjson.h")
#include "simdjson/singleheader/simdjson.h"
#else
#include "simdjson.h"
#endif

#if defined(BOLT_SIMDJSON_RESTORE_CPP_LIB_RANGES)
#pragma pop_macro("__cpp_lib_ranges")
#undef BOLT_SIMDJSON_RESTORE_CPP_LIB_RANGES
#endif

#if defined(BOLT_SIMDJSON_UNDEFINE_DISABLE_RANGES)
#undef SIMDJSON_DISABLE_RANGES
#undef BOLT_SIMDJSON_UNDEFINE_DISABLE_RANGES
#endif
