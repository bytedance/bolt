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
/*
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
#include "bolt/tpcds/gen/dsdgen/include/dist.h"

namespace bytedance::bolt {
class BaseVector;
using VectorPtr = std::shared_ptr<BaseVector>;
} // namespace bytedance::bolt

namespace bytedance::bolt::tpcds {

struct TpcdsTableDef {
  const char* name = "";
  int fl_small = 0;
  int fl_child = 0;
  int first_column = 0;
  int colIndex = 0;
  int rowIndex = 0;
  DSDGenContext* dsdGenContext = nullptr;
  std::vector<bolt::VectorPtr> children = {};
  bool IsNull(int32_t column);
};
} // namespace bytedance::bolt::tpcds
