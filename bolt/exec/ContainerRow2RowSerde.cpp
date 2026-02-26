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

#include "bolt/exec/ContainerRow2RowSerde.h"
namespace bytedance::bolt::exec {
// static
void ContainerRow2RowSerde::serialize(
    char* row,
    char*& current,
    const char* bufferEnd,
    const RowFormatInfo& info) {
  simd::memcpy(current, row, info.fixRowSize);
  char* currentHead = current;
  current += info.fixRowSize;

  for (const auto& [isStr, rowColumn] : info.variableColumns) {
    if (RowContainer::isNullAt(row, rowColumn)) {
      continue;
    }
    if (isStr) {
      const StringView& sv =
          *reinterpret_cast<StringView*>(row + rowColumn.offset());
      if (!sv.isInline()) {
        if (reinterpret_cast<const HashStringAllocator::Header*>(sv.data())[-1]
                .size() >= sv.size()) {
          simd::memcpy(current, sv.data(), sv.size());
        } else {
          auto stream = HashStringAllocator::prepareRead(
              HashStringAllocator::headerOf(sv.data()));
          stream->readBytes(current, sv.size());
        }
        // clear data ptr of StringView to increase compress ratio
        *(char**)(currentHead + rowColumn.offset() + sizeof(uint64_t)) =
            nullptr;
        current += sv.size();
        BOLT_CHECK(current <= bufferEnd);
      }
    } else {
      const auto value =
          *reinterpret_cast<std::string_view*>(row + rowColumn.offset());

      auto stream = HashStringAllocator::prepareRead(
          HashStringAllocator::headerOf(value.data()));
      stream->readBytes(current, value.size());
      current += value.size();
      BOLT_CHECK(current <= bufferEnd);
    }
  }
  for (auto& accumulator : info.serializableAccumulators) {
    current = accumulator.serializeAccumulator(currentHead, current);
    BOLT_CHECK(current <= bufferEnd);
  }

  // make row size aligned with alignment
  auto actualSize = current - currentHead;
  auto rowSize = bits::roundUp(actualSize, info.alignment);
  current += rowSize - actualSize;
}

// static
int32_t ContainerRow2RowSerde::deserialize(
    char*& current,
    const RowFormatInfo& info,
    bool advance) {
  BOLT_DCHECK(reinterpret_cast<std::uintptr_t>(current) % info.alignment == 0);
  char* varBufferStart = current + info.fixRowSize;
  for (const auto& [isStr, rowColumn] : info.variableColumns) {
    if (RowContainer::isNullAt(current, rowColumn)) {
      continue;
    }
    if (isStr) {
      StringView& sv =
          *reinterpret_cast<StringView*>(current + rowColumn.offset());
      if (!sv.isInline()) {
        sv = StringView(varBufferStart, sv.size());
        varBufferStart += sv.size();
      }
    } else {
      auto& value =
          *reinterpret_cast<std::string_view*>(current + rowColumn.offset());
      value = std::string_view(varBufferStart, value.size());
      varBufferStart += value.size();
    }
  }
  for (const auto& accumulator : info.serializableAccumulators) {
    varBufferStart =
        accumulator.deserializeAccumulator(current, varBufferStart);
  }

  auto rowSize = varBufferStart - current;
  auto delta = bits::roundUp(rowSize, info.alignment) - rowSize;
  rowSize += delta;

  // if single row, do not advance current
  if (advance) {
    current = varBufferStart + delta;
  }
  return rowSize;
}

int32_t ContainerRow2RowSerde::rowSize(char* row, const RowFormatInfo& info) {
  // not an efficient way
  return ContainerRow2RowSerde::deserialize(row, info, false);
}

void ContainerRow2RowSerde::copyRow(
    char*& dest,
    const char* src,
    const size_t length,
    const RowFormatInfo& info) {
  memcpy(dest, src, length);
  char* varBufferStart = dest + info.fixRowSize;
  for (const auto& [isStr, rowColumn] : info.variableColumns) {
    if (RowContainer::isNullAt(dest, rowColumn)) {
      continue;
    }
    if (isStr) {
      StringView& sv =
          *reinterpret_cast<StringView*>(dest + rowColumn.offset());
      if (!sv.isInline()) {
        sv = StringView(varBufferStart, sv.size());
        varBufferStart += sv.size();
      }
    } else {
      auto& value =
          *reinterpret_cast<std::string_view*>(dest + rowColumn.offset());
      value = std::string_view(varBufferStart, value.size());
      varBufferStart += value.size();
    }
  }
}

} // namespace bytedance::bolt::exec
