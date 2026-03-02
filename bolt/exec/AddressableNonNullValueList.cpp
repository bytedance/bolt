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

#include "bolt/exec/AddressableNonNullValueList.h"
#include "bolt/exec/ContainerRowSerde.h"
namespace bytedance::bolt::aggregate::prestosql {

AddressableNonNullValueList::Entry AddressableNonNullValueList::append(
    const DecodedVector& decoded,
    vector_size_t index,
    HashStringAllocator* allocator) {
  ByteOutputStream stream(allocator);
  if (!firstHeader_) {
    // An array_agg or related begins with an allocation of 5 words and
    // 4 bytes for header. This is compact for small arrays (up to 5
    // bigints) and efficient if needs to be extended (stores 4 bigints
    // and a next pointer. This could be adaptive, with smaller initial
    // sizes for lots of small arrays.
    static constexpr int kInitialSize = 44;

    currentPosition_ = allocator->newWrite(stream, kInitialSize);
    firstHeader_ = currentPosition_.header;
  } else {
    allocator->extendWrite(currentPosition_, stream);
  }

  const auto hash = decoded.base()->hashValueAt(decoded.index(index));

  const auto originalSize = stream.size();

  // Write value.
  exec::ContainerRowSerdeOptions options{};
  exec::ContainerRowSerde::serialize(
      *decoded.base(), decoded.index(index), stream, options);

  ++size_;

  auto startAndFinish = allocator->finishWrite(stream, 1024);
  currentPosition_ = startAndFinish.second;

  const auto writtenSize = stream.size() - originalSize;

  return {startAndFinish.first, writtenSize, hash};
}

namespace {

std::unique_ptr<ByteInputStream> prepareRead(
    const AddressableNonNullValueList::Entry& entry) {
  auto header = entry.offset.header;
  auto seek = entry.offset.position - header->begin();

  auto stream = HashStringAllocator::prepareRead(header, entry.size + seek);
  stream->seekp(seek);
  return stream;
}
} // namespace

// static
bool AddressableNonNullValueList::equalTo(
    const Entry& left,
    const Entry& right,
    const TypePtr& type) {
  if (left.hash != right.hash) {
    return false;
  }

  auto leftStream = prepareRead(left);
  auto rightStream = prepareRead(right);

  CompareFlags compareFlags =
      CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);
  return exec::ContainerRowSerde::compare(
             *leftStream, *rightStream, type.get(), compareFlags) == 0;
}

// static
void AddressableNonNullValueList::read(
    const Entry& position,
    BaseVector& result,
    vector_size_t index) {
  auto stream = prepareRead(position);
  exec::ContainerRowSerde::deserialize(*stream, index, &result);
}

} // namespace bytedance::bolt::aggregate::prestosql
