/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/common/base/Exceptions.h"

#include <cstdint>

namespace bytedance::bolt::exec::bm::benchmarks {

enum class DatasetKind {
  kFixed,
  kVariableSmall,
  kVariableLarge,
  kBigint,
  kInteger,
  kDouble,
  kVarcharSmall,
  kVarcharLarge,
  kArray,
  kMap,
  kRow,
};

struct StringProfileOptions {
  uint32_t variableMaxStringLength{64};
  uint32_t largeStringLength{1024};
};

bool hasVariableColumn(DatasetKind dataset);

const char* datasetName(DatasetKind dataset);

uint32_t stringLengthForRow(
    DatasetKind dataset,
    uint64_t row,
    const StringProfileOptions& options);

uint64_t estimatedStringBytesPerRow(
    DatasetKind dataset,
    const StringProfileOptions& options);

} // namespace bytedance::bolt::exec::bm::benchmarks
