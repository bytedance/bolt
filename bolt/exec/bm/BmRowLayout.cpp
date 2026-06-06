#include "bolt/exec/bm/BmRowLayout.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorTypeUtils.h"

#include <algorithm>
#include <utility>

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

BmRowLayout::BmRowLayout(
    std::vector<TypePtr> keyTypes,
    std::vector<TypePtr> dependentTypes,
    int32_t alignment)
    : keyTypes_(std::move(keyTypes)),
      dependentTypes_(std::move(dependentTypes)),
      alignment_(alignment) {
  compute();
}

void BmRowLayout::compute() {
  int32_t offset = 0;
  int32_t nullOffset = 0;
  for (auto& type : keyTypes_) {
    types_.push_back(type);
    typeKinds_.push_back(type->kind());
    offsets_.push_back(offset);
    offset += typeKindSize(type->kind());
    nullOffsets_.push_back(nullOffset++);
    hasVariableWidth_ |= !type->isFixedWidth();
  }

  offset = std::max<int32_t>(offset, sizeof(void*));
  firstNullByteOffset_ = offset;

  for (auto& type : dependentTypes_) {
    types_.push_back(type);
    typeKinds_.push_back(type->kind());
    nullOffsets_.push_back(nullOffset++);
    hasVariableWidth_ |= !type->isFixedWidth();
  }

  nullOffsets_.push_back(nullOffset);
  freeFlagOffset_ = nullOffset + firstNullByteOffset_ * 8;
  ++nullOffset;

  for (auto& null : nullOffsets_) {
    null += firstNullByteOffset_ * 8;
  }

  const int32_t nullBytes = bits::nbytes(nullOffsets_.size());
  offset += nullBytes;

  for (auto& type : dependentTypes_) {
    offsets_.push_back(offset);
    offset += typeKindSize(type->kind());
  }

  if (hasVariableWidth_) {
    rowSizeOffset_ = offset;
    offset += sizeof(uint32_t);
  }

  fixedRowSize_ = bits::roundUp(offset, alignment_);
  if (!nullOffsets_.empty()) {
    initialNulls_.resize(nullBytes, 0x0);
  }

  for (auto i = 0; i < offsets_.size(); ++i) {
    columns_.emplace_back(offsets_[i], nullOffsets_[i]);
  }
}

} // namespace bytedance::bolt::exec
