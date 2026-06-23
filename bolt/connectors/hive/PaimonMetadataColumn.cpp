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
#include "bolt/connectors/hive/PaimonMetadataColumn.h"
#include "bolt/connectors/hive/HiveConnectorUtil.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::connector::paimon {

TypePtr MetadataColumnFilePath::type() const {
  return VARCHAR();
}

void MetadataColumnFilePath::populateVector(VectorPtr& vector) {
  auto size = vector->size();
  vector = BaseVector::createConstant(type(), filePath_, size, vector->pool());
}

TypePtr MetadataColumnBucket::type() const {
  return INTEGER();
}

void MetadataColumnBucket::populateVector(VectorPtr& vector) {
  auto size = vector->size();
  VectorPtr bucketVector;
  vector = BaseVector::createConstant(type(), bucket_, size, vector->pool());
}

TypePtr MetadataColumnPartition::type() const {
  return partitionType_;
}

MetadataColumnPartition::MetadataColumnPartition(
    TypePtr partitionType,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKeys,
    memory::MemoryPool* pool)
    : partitionType_(std::move(partitionType)) {
  std::vector<VectorPtr> childVectors;
  const auto& partitionRowType = partitionType_->asRow();
  childVectors.reserve(partitionRowType.size());

  for (uint32_t i = 0; i < partitionRowType.size(); ++i) {
    const auto& key = partitionRowType.nameOf(i);
    const auto& fieldType = partitionRowType.childAt(i);
    const auto it = partitionKeys.find(key);

    if (it != partitionKeys.end() && it->second.has_value()) {
      auto convertedValue = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          hive::convertFromString, fieldType->kind(), it->second);
      childVectors.push_back(
          BaseVector::createConstant(fieldType, convertedValue, 1, pool));
    } else {
      childVectors.push_back(
          BaseVector::createNullConstant(fieldType, 1, pool));
    }
  }

  partitionValue_ = std::make_shared<RowVector>(
      pool,
      asRowType(partitionType_),
      BufferPtr(nullptr),
      1, // size
      std::move(childVectors));
}

void MetadataColumnPartition::populateVector(VectorPtr& vector) {
  auto size = vector->size();
  vector = BaseVector::wrapInConstant(size, 0, partitionValue_, true);
}

TypePtr MetadataColumnSequenceNumber::type() const {
  return BIGINT();
}

void MetadataColumnSequenceNumber::populateVector(VectorPtr& vector) {
  auto size = vector->size();
  vector = BaseVector::createConstant(
      type(), maxSequenceNumber_, size, vector->pool());
}

} // namespace bytedance::bolt::connector::paimon
