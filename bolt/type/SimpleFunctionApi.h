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
#pragma once

#include <cstddef>
#include <string>

#include <fmt/core.h>

#include "bolt/type/Type.h"

namespace bytedance::bolt {

template <size_t id>
struct IntegerVariable {
  static size_t getId() {
    return id;
  }

  static std::string name() {
    return fmt::format("i{}", id);
  }
};

using P1 = IntegerVariable<1>;
using P2 = IntegerVariable<2>;
using P3 = IntegerVariable<3>;
using P4 = IntegerVariable<4>;
using S1 = IntegerVariable<5>;
using S2 = IntegerVariable<6>;
using S3 = IntegerVariable<7>;
using S4 = IntegerVariable<8>;

template <size_t id>
struct EnumVariable {
  static size_t getId() {
    return id;
  }

  static std::string name() {
    return fmt::format("E{}", id);
  }
};

using E1 = EnumVariable<1>;
using E2 = EnumVariable<2>;

template <typename P, typename S>
struct ShortDecimal {
 private:
  ShortDecimal() {}
};

template <typename P, typename S>
struct LongDecimal {
 private:
  LongDecimal() {}
};

template <typename P, typename S>
struct SimpleTypeTrait<ShortDecimal<P, S>> : public SimpleTypeTrait<int64_t> {
  static constexpr const char* name = "DECIMAL";
};

template <typename P, typename S>
struct SimpleTypeTrait<LongDecimal<P, S>> : public SimpleTypeTrait<int128_t> {
  static constexpr const char* name = "DECIMAL";
};

template <typename P, typename S>
struct CppToType<ShortDecimal<P, S>> : public CppToTypeBase<TypeKind::BIGINT> {
};

template <typename P, typename S>
struct CppToType<LongDecimal<P, S>> : public CppToTypeBase<TypeKind::HUGEINT> {
};

} // namespace bytedance::bolt
