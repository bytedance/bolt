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

#include "bolt/type/Type.h"
namespace bytedance::bolt {

namespace exec {
class CastOperator;
}

// Given a class T, create a custom type that can be used to refer to it, with
// an underlying opaque vector type.
// Note that name must be stored in a variable and passed and can't be inlined.
// OpaqueCustomTypeRegister<T, "type"> won't compile.
// but static constexpr char* type = "type", OpaqueCustomTypeRegister<T, type>
// works.
template <typename T, const char* customTypeName>
class OpaqueCustomTypeRegister {
 public:
  static void registerType() {
    bytedance::bolt::registerCustomType(
        customTypeName, std::make_unique<const TypeFactory>());
  }

  static bool unregisterType() {
    return bytedance::bolt::unregisterCustomType(customTypeName);
  }

  // Type used in the simple function interface as CustomType<TypeT>.
  struct TypeT {
    using type = std::shared_ptr<T>;
    static constexpr const char* typeName = customTypeName;
  };

  using SimpleType = CustomType<TypeT>;

  class BoltType : public OpaqueType {
   public:
    BoltType() : OpaqueType(std::type_index(typeid(T))) {}

    static const TypePtr& get() {
      static const TypePtr instance{new BoltType()};
      return instance;
    }

    static const std::shared_ptr<const exec::CastOperator>& getCastOperator() {
      BOLT_UNSUPPORTED();
    }

    bool equivalent(const bolt::Type& other) const override {
      // Pointer comparison works since this type is a singleton.
      return this == &other;
    }

    const char* name() const override {
      return customTypeName;
    }

    std::string toString() const override {
      return customTypeName;
    }
  };

  static const TypePtr& singletonTypePtr() {
    return BoltType::get();
  }

 private:
  class TypeFactory : public CustomTypeFactories {
   public:
    TypeFactory() = default;

    TypePtr getType() const override {
      return singletonTypePtr();
    }

    exec::CastOperatorPtr getCastOperator() const override {
      BOLT_UNSUPPORTED();
    }
  };
};
} // namespace bytedance::bolt
