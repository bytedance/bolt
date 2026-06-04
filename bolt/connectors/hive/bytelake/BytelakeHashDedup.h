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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <folly/container/F14Map.h>

#include "bolt/connectors/hive/bytelake/BytelakeRowOrigin.h"
#include "bolt/connectors/hive/bytelake/BytelakeScanSpecUtil.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::connector::hive {

// Streaming hash-based dedup for bytelake two-pass Phase 1+2. PK is packed
// into a std::string (via BytelakeKeyPacker) used as the F14FastMap key;
// per-PK winner is the row with max precombine, others are recorded as
// losers per file.
class BytelakeHashDedup {
 public:
  // schema layout: PK columns, then precombine columns, then isDeleted
  // (last). numFiles sizes the per-file losers vector; addBatch's fileIdx
  // must be < numFiles.
  BytelakeHashDedup(const BytelakeKeyOnlySchema& schema, size_t numFiles);

  // Process one batch from file `fileIdx`. Key columns must be
  // force-materialized. batchStartRow is the file row of batch[0] (requires
  // filter-free Phase 1 ScanSpec for cumulative sizes to match physical
  // file positions).
  void
  addBatch(const RowVectorPtr& batch, uint32_t fileIdx, uint32_t batchStartRow);

  // Move-out losers per file. Includes both losers from PK contest AND
  // deleted-tombstone winners (Hudi Merge-on-Read: deleted PKs must not appear).
  // Order within each per-file vector is unspecified.
  std::vector<std::vector<uint32_t>> takeLosers() &&;

  size_t numWinners() const {
    return table_.size();
  }

 private:
  struct BestEntry {
    std::string packedPrecombine;
    BytelakeRowOrigin origin;
    bool isDeleted;
  };

  const int numPkColumns_;
  const int numPrecombineColumns_;
  RowTypePtr keyRowType_;

  folly::F14FastMap<std::string, BestEntry> table_;
  std::vector<std::vector<uint32_t>> losersPerFile_;
};

} // namespace bytedance::bolt::connector::hive
