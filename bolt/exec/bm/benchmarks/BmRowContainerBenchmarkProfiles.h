#pragma once

#include "bolt/common/base/Exceptions.h"

#include <cstdint>

namespace bytedance::bolt::exec::bm::benchmarks {

enum class DatasetKind {
  kFixed,
  kVariableSmall,
  kVariableLarge,
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
