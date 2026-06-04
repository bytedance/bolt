/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/connectors/hive/bytelake/BytelakeHashDedup.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/connectors/hive/bytelake/BytelakeKeyPacker.h"
#include "bolt/vector/SelectivityVector.h"

namespace bytedance::bolt::connector::hive {

BytelakeHashDedup::BytelakeHashDedup(
    const BytelakeKeyOnlySchema& schema,
    size_t numFiles)
    : numPkColumns_(schema.numPkColumns),
      numPrecombineColumns_(schema.numPrecombineColumns),
      keyRowType_(schema.rowType),
      losersPerFile_(numFiles) {
  BOLT_CHECK_GT(
      numPkColumns_, 0, "BytelakeHashDedup requires at least 1 PK column");
  BOLT_CHECK_GE(numPrecombineColumns_, 0);
  BOLT_CHECK_NOT_NULL(keyRowType_);
  BOLT_CHECK_EQ(
      keyRowType_->size(),
      static_cast<size_t>(numPkColumns_ + numPrecombineColumns_ + 1),
      "keyRowType size must equal numPk + numPrecombine + 1 (for isDeleted)");
}

void BytelakeHashDedup::addBatch(
    const RowVectorPtr& batch,
    uint32_t fileIdx,
    uint32_t batchStartRow) {
  BOLT_CHECK_NOT_NULL(batch);
  BOLT_CHECK_LT(
      static_cast<size_t>(fileIdx),
      losersPerFile_.size(),
      "fileIdx out of range for losersPerFile_");

  const vector_size_t n = batch->size();
  if (n == 0) {
    return;
  }

  SelectivityVector allRows(n);

  std::vector<DecodedVector> pkDecoded(numPkColumns_);
  std::vector<const DecodedVector*> pkDecoders(numPkColumns_);
  std::vector<TypeKind> pkKinds(numPkColumns_);
  for (int i = 0; i < numPkColumns_; ++i) {
    pkDecoded[i].decode(*batch->childAt(i), allRows);
    pkDecoders[i] = &pkDecoded[i];
    pkKinds[i] = keyRowType_->childAt(i)->kind();
  }

  std::vector<DecodedVector> pcDecoded(numPrecombineColumns_);
  std::vector<const DecodedVector*> pcDecoders(numPrecombineColumns_);
  std::vector<TypeKind> pcKinds(numPrecombineColumns_);
  for (int i = 0; i < numPrecombineColumns_; ++i) {
    pcDecoded[i].decode(*batch->childAt(numPkColumns_ + i), allRows);
    pcDecoders[i] = &pcDecoded[i];
    pcKinds[i] = keyRowType_->childAt(numPkColumns_ + i)->kind();
  }

  DecodedVector isDeletedDecoded;
  const int isDeletedChannel = numPkColumns_ + numPrecombineColumns_;
  isDeletedDecoded.decode(*batch->childAt(isDeletedChannel), allRows);

  for (vector_size_t row = 0; row < n; ++row) {
    std::string pk = packCompositeKey(pkDecoders, pkKinds, row);
    std::string pc = packCompositeKey(pcDecoders, pcKinds, row);
    // Null isDeleted treated as not-deleted (old files without the column).
    const bool isDel =
        !isDeletedDecoded.isNullAt(row) && isDeletedDecoded.valueAt<bool>(row);
    const BytelakeRowOrigin origin{
        fileIdx, batchStartRow + static_cast<uint32_t>(row)};

    auto it = table_.find(pk);
    if (it == table_.end()) {
      table_.emplace(std::move(pk), BestEntry{std::move(pc), origin, isDel});
    } else if (pc > it->second.packedPrecombine) {
      // New row wins; old winner becomes a loser.
      const auto displaced = it->second.origin;
      losersPerFile_[displaced.fileIdx].push_back(displaced.rowInFile);
      it->second = BestEntry{std::move(pc), origin, isDel};
    } else {
      // Tie or lower: first-seen wins (Hudi default).
      losersPerFile_[fileIdx].push_back(origin.rowInFile);
    }
  }
}

std::vector<std::vector<uint32_t>> BytelakeHashDedup::takeLosers() && {
  // Hudi Merge-on-Read: tombstone winners (latest version = delete) must not appear
  // in output. Fold them into losersPerFile_ so Phase 3's bitmap skips
  // them — production plans omit `WHERE NOT _hoodie_is_deleted` downstream.
  for (const auto& kv : table_) {
    if (kv.second.isDeleted) {
      losersPerFile_[kv.second.origin.fileIdx].push_back(
          kv.second.origin.rowInFile);
    }
  }
  return std::move(losersPerFile_);
}

} // namespace bytedance::bolt::connector::hive
