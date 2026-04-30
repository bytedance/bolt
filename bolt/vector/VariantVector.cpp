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
