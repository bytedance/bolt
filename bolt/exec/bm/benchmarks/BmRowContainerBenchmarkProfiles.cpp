#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkProfiles.h"

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint32_t requirePositive(uint32_t value, const char* name) {
  BOLT_CHECK_GT(value, 0, "{} must be greater than zero.", name);
  return value;
}

} // namespace

bool hasVariableColumn(DatasetKind dataset) {
  return dataset != DatasetKind::kFixed;
}

const char* datasetName(DatasetKind dataset) {
  switch (dataset) {
    case DatasetKind::kFixed:
      return "fixed";
    case DatasetKind::kVariable:
      return "variable";
    case DatasetKind::kVariableLarge:
      return "variable_large";
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
    case DatasetKind::kVariable: {
      const auto maxLength = requirePositive(
          options.variableMaxStringLength, "variableMaxStringLength");
      return 1 + static_cast<uint32_t>(row % maxLength);
    }
    case DatasetKind::kVariableLarge:
      return requirePositive(options.largeStringLength, "largeStringLength");
  }
  BOLT_UNREACHABLE();
}

uint64_t estimatedStringBytesPerRow(
    DatasetKind dataset,
    const StringProfileOptions& options) {
  switch (dataset) {
    case DatasetKind::kFixed:
      return 0;
    case DatasetKind::kVariable: {
      const auto maxLength = requirePositive(
          options.variableMaxStringLength, "variableMaxStringLength");
      return (static_cast<uint64_t>(maxLength) + 2) / 2;
    }
    case DatasetKind::kVariableLarge:
      return requirePositive(options.largeStringLength, "largeStringLength");
  }
  BOLT_UNREACHABLE();
}

} // namespace bytedance::bolt::exec::bm::benchmarks
