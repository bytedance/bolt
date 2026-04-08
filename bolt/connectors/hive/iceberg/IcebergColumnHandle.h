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
 * 2026-03-30.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */
#pragma once

#include <string>
#include <vector>

#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/dwio/parquet/ParquetFieldId.h"
#include "bolt/type/Subfield.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::connector::hive::iceberg {

class IcebergColumnHandle : public HiveColumnHandle {
 public:
  IcebergColumnHandle(
      const std::string& name,
      ColumnType columnType,
      TypePtr dataType,
      parquet::ParquetFieldId icebergField,
      std::vector<common::Subfield> requiredSubfields = {});

  const parquet::ParquetFieldId& field() const;

 private:
  const parquet::ParquetFieldId field_;
};

using IcebergColumnHandlePtr = std::shared_ptr<const IcebergColumnHandle>;

} // namespace bytedance::bolt::connector::hive::iceberg
