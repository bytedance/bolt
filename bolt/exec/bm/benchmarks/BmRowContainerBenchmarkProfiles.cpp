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

#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkProfiles.h"

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint32_t requirePositive(uint32_t value, const char* name) {
  BOLT_CHECK_GT(value, 0, "{} must be greater than zero.", name);
  return value;
}

} // namespace

bool hasVariableColumn(DatasetKind dataset) {
  return dataset == DatasetKind::kVariableSmall ||
      dataset == DatasetKind::kVariableLarge;
}

const char* datasetName(DatasetKind dataset) {
  switch (dataset) {
    case DatasetKind::kFixed:
      return "fixed";
    case DatasetKind::kVariableSmall:
      return "variable_small";
    case DatasetKind::kVariableLarge:
      return "variable_large";
    case DatasetKind::kBigint:
      return "bigint";
    case DatasetKind::kInteger:
      return "integer";
    case DatasetKind::kDouble:
      return "double";
    case DatasetKind::kVarcharSmall:
      return "varchar_small";
    case DatasetKind::kVarcharLarge:
      return "varchar_large";
    case DatasetKind::kArray:
      return "array";
    case DatasetKind::kMap:
      return "map";
    case DatasetKind::kRow:
      return "row";
  }
  BOLT_UNREACHABLE();
}

uint32_t stringLengthForRow(
    DatasetKind dataset,
    uint64_t row,
    const StringProfileOptions& options) {
  switch (dataset) {
    case DatasetKind::kFixed:
      return 0;
    case DatasetKind::kVariableSmall:
    case DatasetKind::kVarcharSmall: {
      const auto maxLength = requirePositive(
          options.variableMaxStringLength, "variableMaxStringLength");
      return 1 + static_cast<uint32_t>(row % maxLength);
    }
    case DatasetKind::kVariableLarge:
    case DatasetKind::kVarcharLarge:
      return requirePositive(options.largeStringLength, "largeStringLength");
    case DatasetKind::kBigint:
    case DatasetKind::kInteger:
    case DatasetKind::kDouble:
    case DatasetKind::kArray:
    case DatasetKind::kMap:
    case DatasetKind::kRow:
      return 0;
  }
  BOLT_UNREACHABLE();
}

uint64_t estimatedStringBytesPerRow(
    DatasetKind dataset,
    const StringProfileOptions& options) {
  switch (dataset) {
    case DatasetKind::kFixed:
      return 0;
    case DatasetKind::kVariableSmall:
    case DatasetKind::kVarcharSmall: {
      const auto maxLength = requirePositive(
          options.variableMaxStringLength, "variableMaxStringLength");
      return (static_cast<uint64_t>(maxLength) + 2) / 2;
    }
    case DatasetKind::kVariableLarge:
    case DatasetKind::kVarcharLarge:
      return requirePositive(options.largeStringLength, "largeStringLength");
    case DatasetKind::kBigint:
    case DatasetKind::kInteger:
    case DatasetKind::kDouble:
    case DatasetKind::kArray:
    case DatasetKind::kMap:
    case DatasetKind::kRow:
      return 0;
  }
  BOLT_UNREACHABLE();
}

} // namespace bytedance::bolt::exec::bm::benchmarks
