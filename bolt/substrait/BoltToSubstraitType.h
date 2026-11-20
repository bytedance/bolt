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

#include "bolt/core/PlanNode.h"

#include "bolt/substrait/SubstraitExtensionCollector.h"
#include "bolt/substrait/proto/substrait/algebra.pb.h"
#include "bolt/substrait/proto/substrait/type.pb.h"
namespace bytedance::bolt::substrait {

class BoltToSubstraitTypeConvertor {
 public:
  explicit BoltToSubstraitTypeConvertor(
      const SubstraitExtensionCollectorPtr& extensionCollector = nullptr)
      : extensionCollector_(
            extensionCollector
                ? extensionCollector
                : std::make_shared<SubstraitExtensionCollector>()) {}

  /// Convert Bolt RowType to Substrait NamedStruct.
  const ::substrait::NamedStruct& toSubstraitNamedStruct(
      google::protobuf::Arena& arena,
      const bolt::RowTypePtr& rowType);

  /// Convert Bolt Type to Substrait Type.
  const ::substrait::Type& toSubstraitType(
      google::protobuf::Arena& arena,
      const bolt::TypePtr& type);

 private:
  SubstraitExtensionCollectorPtr extensionCollector_;
};

bolt::core::JoinType toBoltJoinType(
    ::substrait::HashJoinRel::JoinType joinType);

using BoltToSubstraitTypeConvertorPtr =
    std::shared_ptr<BoltToSubstraitTypeConvertor>;

} // namespace bytedance::bolt::substrait
