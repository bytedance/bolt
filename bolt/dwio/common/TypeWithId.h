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
#include <unordered_map>
#include <vector>

#include "bolt/type/Type.h"
namespace bytedance::bolt::dwio::common {

class TypeWithId : public bolt::Tree<std::shared_ptr<const TypeWithId>> {
 public:
  TypeWithId(
      std::shared_ptr<const bolt::Type> type,
      std::vector<std::shared_ptr<const TypeWithId>>&& children,
      uint32_t id,
      uint32_t maxId,
      uint32_t column);

  static std::shared_ptr<const TypeWithId> create(
      const std::shared_ptr<const bolt::Type>& root,
      uint32_t next = 0);

  uint32_t size() const override;

  const std::shared_ptr<const bolt::Type>& type() const {
    return type_;
  }

  const TypeWithId* parent() const {
    return parent_;
  }

  uint32_t id() const {
    return id_;
  }

  uint32_t maxId() const {
    return maxId_;
  }

  uint32_t column() const {
    return column_;
  }

  const std::shared_ptr<const TypeWithId>& childAt(uint32_t idx) const override;

  bool containsChild(const std::string& name) const {
    BOLT_CHECK_EQ(type_->kind(), bolt::TypeKind::ROW);
    return type_->as<bolt::TypeKind::ROW>().containsChild(name);
  }

  const std::shared_ptr<const TypeWithId>& childByName(
      const std::string& name) const {
    BOLT_CHECK_EQ(type_->kind(), bolt::TypeKind::ROW);
    return childAt(type_->as<bolt::TypeKind::ROW>().getChildIdx(name));
  }

  const std::vector<std::shared_ptr<const TypeWithId>>& getChildren() const {
    return children_;
  }

  virtual bool isDCMap() const {
    return false;
  }

  virtual const std::unordered_map<std::string, std::vector<std::string>>&
  getDcKeys() const {
    static const std::unordered_map<std::string, std::vector<std::string>>
        EMPTY_MAP;
    return EMPTY_MAP;
  }

 private:
  static std::shared_ptr<const TypeWithId> create(
      const std::shared_ptr<const bolt::Type>& type,
      uint32_t& next,
      uint32_t column);

  const std::shared_ptr<const bolt::Type> type_;
  const TypeWithId* const parent_;
  const uint32_t id_;
  const uint32_t maxId_;
  const uint32_t column_;
  const std::vector<std::shared_ptr<const TypeWithId>> children_;
};

} // namespace bytedance::bolt::dwio::common
