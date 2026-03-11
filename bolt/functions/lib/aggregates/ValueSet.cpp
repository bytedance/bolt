/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/functions/lib/aggregates/ValueSet.h"
#include "bolt/exec/ContainerRowSerde.h"
namespace bytedance::bolt::aggregate {

void ValueSet::write(
    const BaseVector& vector,
    vector_size_t index,
    HashStringAllocator::Position& position) const {
  ByteOutputStream stream(allocator_);
  if (position.header == nullptr) {
    position = allocator_->newWrite(stream);
  } else {
    allocator_->extendWrite(position, stream);
  }

  // Set isKey to false to avoid unnecessary sorting.
  static const exec::ContainerRowSerdeOptions kOptions{/*isKey=*/false};
  exec::ContainerRowSerde::serialize(vector, index, stream, kOptions);
  allocator_->finishWrite(stream, 0);
}

StringView ValueSet::write(const StringView& value) const {
  if (value.isInline()) {
    return value;
  }

  const auto size = value.size();

  auto* header = allocator_->allocate(size);
  auto* start = header->begin();

  memcpy(start, value.data(), size);
  return StringView(start, size);
}

void ValueSet::read(
    BaseVector* vector,
    vector_size_t index,
    const HashStringAllocator::Header* header) const {
  BOLT_CHECK_NOT_NULL(header);

  auto stream = HashStringAllocator::prepareRead(header);
  exec::ContainerRowSerde::deserialize(*stream, index, vector);
}

void ValueSet::free(HashStringAllocator::Header* header) const {
  BOLT_CHECK_NOT_NULL(header);
  allocator_->free(header);
}

void ValueSet::free(const StringView& value) const {
  if (!value.isInline()) {
    auto* header = HashStringAllocator::headerOf(value.data());
    allocator_->free(header);
  }
}

} // namespace bytedance::bolt::aggregate
