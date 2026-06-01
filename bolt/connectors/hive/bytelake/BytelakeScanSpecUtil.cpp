/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/connectors/hive/bytelake/BytelakeScanSpecUtil.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::connector::hive {

BytelakeKeyOnlySchema makeBytelakeKeyOnlySchema(
    const RowTypePtr& fullRowType,
    const std::vector<int>& pkChannelsInFullSchema,
    const std::vector<int>& precombineChannelsInFullSchema,
    int isDeletedChannelInFullSchema) {
  BOLT_CHECK_NOT_NULL(fullRowType);
  BOLT_CHECK(
      !pkChannelsInFullSchema.empty(),
      "Bytelake requires at least one PK column");

  const size_t totalSize = pkChannelsInFullSchema.size() +
      precombineChannelsInFullSchema.size() + 1;

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(totalSize);
  types.reserve(totalSize);

  auto spec = std::make_shared<common::ScanSpec>("root");

  auto appendColumn = [&](int srcChannel) {
    BOLT_CHECK_GE(srcChannel, 0);
    BOLT_CHECK_LT(srcChannel, fullRowType->size());
    const auto& name = fullRowType->nameOf(srcChannel);
    const auto& type = fullRowType->childAt(srcChannel);
    const auto dstChannel = static_cast<column_index_t>(names.size());
    names.push_back(name);
    types.push_back(type);
    spec->addFieldRecursively(name, *type, dstChannel);
  };

  // Layout: [PK..., precombine..., isDeleted]
  for (int ch : pkChannelsInFullSchema) {
    appendColumn(ch);
  }
  for (int ch : precombineChannelsInFullSchema) {
    appendColumn(ch);
  }
  appendColumn(isDeletedChannelInFullSchema);

  BytelakeKeyOnlySchema schema;
  schema.rowType = ROW(std::move(names), std::move(types));
  schema.scanSpec = std::move(spec);
  schema.numPkColumns = static_cast<int>(pkChannelsInFullSchema.size());
  schema.numPrecombineColumns =
      static_cast<int>(precombineChannelsInFullSchema.size());
  return schema;
}

} // namespace bytedance::bolt::connector::hive
