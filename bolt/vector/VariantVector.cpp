/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/vector/VariantVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt {

std::unique_ptr<SimpleVector<uint64_t>> VariantVector::hashAll() const {
  BufferPtr hashBuffer = AlignedBuffer::allocate<uint64_t>(length_, pool_);
  auto hashes = std::make_unique<FlatVector<uint64_t>>(
      pool_,
      BIGINT(),
      nullptr,
      length_,
      std::move(hashBuffer),
      std::vector<BufferPtr>());
  auto rawHashes = hashes->mutableRawValues();
  for (vector_size_t i = 0; i < length_; ++i) {
    rawHashes[i] = hashValueAt(i);
  }
  return hashes;
}

} // namespace bytedance::bolt
