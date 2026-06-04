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

#pragma once

#include <memory>
#include <vector>

#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::connector::hive {

// Reduced (key-only) reader schema + matching ScanSpec for Phase 1 read.
// Layout (channels 0..rowType->size()-1):
//   [0, numPkColumns)                         PK columns
//   [numPkColumns, +numPrecombineColumns)     precombine columns
//   [last]                                    isDeleted (_hoodie_is_deleted)
struct BytelakeKeyOnlySchema {
  RowTypePtr rowType;
  std::shared_ptr<common::ScanSpec> scanSpec;
  int numPkColumns;
  int numPrecombineColumns;
};

// Pack PK/precombine/isDeleted channels from a wide fullRowType into a
// compact key-only rowType + filter-free ScanSpec, in the layout above.
// Caller guarantees: channels unique and in fullRowType bounds; at least
// one PK channel.
BytelakeKeyOnlySchema makeBytelakeKeyOnlySchema(
    const RowTypePtr& fullRowType,
    const std::vector<int>& pkChannelsInFullSchema,
    const std::vector<int>& precombineChannelsInFullSchema,
    int isDeletedChannelInFullSchema);

} // namespace bytedance::bolt::connector::hive
