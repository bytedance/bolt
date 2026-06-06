#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorTypeUtils.h"

#include <algorithm>

namespace bytedance::bolt::exec {
namespace {

template <TypeKind Kind>
int32_t kindSize() {
  return sizeof(typename KindToFlatVector<Kind>::HashRowType);
}

int32_t typeKindSize(TypeKind kind) {
  if (kind == TypeKind::UNKNOWN) {
    return sizeof(UnknownValue);
  }
  if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
    return sizeof(VarData);
  }
  return BOLT_DYNAMIC_TYPE_DISPATCH(kindSize, kind);
}

} // namespace

void BmRowContainer::computeLayout() {
  int32_t offset = 0;
  int32_t nullOffset = 0;
  bool isVariableWidth = false;
  for (auto& type : keyTypes_) {
    types_.push_back(type);
    typeKinds_.push_back(type->kind());
    offsets_.push_back(offset);
    offset += typeKindSize(type->kind());
    nullOffsets_.push_back(nullOffset++);
    isVariableWidth |= !type->isFixedWidth();
  }

  offset = std::max<int32_t>(offset, sizeof(void*));
  const int32_t firstNullByteOffset = offset;

  for (auto& type : dependentTypes_) {
    types_.push_back(type);
    typeKinds_.push_back(type->kind());
    nullOffsets_.push_back(nullOffset++);
    isVariableWidth |= !type->isFixedWidth();
  }

  nullOffsets_.push_back(nullOffset);
  freeFlagOffset_ = nullOffset + firstNullByteOffset * 8;
  ++nullOffset;

  for (auto& null : nullOffsets_) {
    null += firstNullByteOffset * 8;
  }

  const int32_t nullBytes = bits::nbytes(nullOffsets_.size());
  offset += nullBytes;

  for (auto& type : dependentTypes_) {
    offsets_.push_back(offset);
    offset += typeKindSize(type->kind());
  }

  if (isVariableWidth) {
    rowSizeOffset_ = offset;
    offset += sizeof(uint32_t);
  }

  fixedRowSize_ = bits::roundUp(offset, alignment_);
  if (!nullOffsets_.empty()) {
    initialNulls_.resize(nullBytes, 0x0);
  }

  for (auto i = 0; i < offsets_.size(); ++i) {
    rowColumns_.emplace_back(offsets_[i], nullOffsets_[i]);
  }
}

} // namespace bytedance::bolt::exec
