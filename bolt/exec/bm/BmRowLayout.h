#pragma once

#include "bolt/exec/bm/BmRowTypes.h"
#include "bolt/type/Type.h"

#include <cstdint>
#include <vector>

namespace bytedance::bolt::exec {

class BmRowLayout {
 public:
  BmRowLayout(
      std::vector<TypePtr> keyTypes,
      std::vector<TypePtr> dependentTypes,
      int32_t alignment = 8);

  int32_t numColumns() const {
    return static_cast<int32_t>(columns_.size());
  }

  int32_t fixedRowSize() const {
    return fixedRowSize_;
  }

  int32_t alignment() const {
    return alignment_;
  }

  bool hasVariableWidth() const {
    return hasVariableWidth_;
  }

  int32_t rowSizeOffset() const {
    return rowSizeOffset_;
  }

  int32_t freeFlagOffset() const {
    return freeFlagOffset_;
  }

  int32_t firstNullByteOffset() const {
    return firstNullByteOffset_;
  }

  TypeKind typeKindAt(int32_t column) const {
    return typeKinds_.at(column);
  }

  const TypePtr& typeAt(int32_t column) const {
    return types_.at(column);
  }

  const BmRowColumn& columnAt(int32_t column) const {
    return columns_.at(column);
  }

  const std::vector<BmRowColumn>& columns() const {
    return columns_;
  }

  const std::vector<TypePtr>& columnTypes() const {
    return types_;
  }

  const std::vector<TypePtr>& keyTypes() const {
    return keyTypes_;
  }

  const std::vector<char>& initialNulls() const {
    return initialNulls_;
  }

 private:
  void compute();

  std::vector<TypePtr> keyTypes_;
  std::vector<TypePtr> dependentTypes_;
  std::vector<TypePtr> types_;
  std::vector<TypeKind> typeKinds_;
  std::vector<int32_t> offsets_;
  std::vector<int32_t> nullOffsets_;
  std::vector<BmRowColumn> columns_;
  std::vector<char> initialNulls_;
  int32_t alignment_;
  int32_t fixedRowSize_{0};
  int32_t rowSizeOffset_{0};
  int32_t freeFlagOffset_{0};
  int32_t firstNullByteOffset_{0};
  bool hasVariableWidth_{false};
};

} // namespace bytedance::bolt::exec
