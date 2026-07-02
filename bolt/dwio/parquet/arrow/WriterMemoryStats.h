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

#pragma once

#include <cstdint>
#include <sstream>
#include <string>

#include "bolt/common/base/SuccinctPrinter.h"

namespace bytedance::bolt::parquet::arrow {

struct WriterMemoryStats {
  int64_t encoderCurrentBytes{0};
  int64_t encoderEstimatedDataEncodedBytes{0};
  int64_t dictEncoderCurrentBytes{0};
  int64_t dictEncoderEstimatedDataEncodedBytes{0};
  int64_t bufferedPageAllocatedBytes{0};
  int64_t bufferedPageActualMemoryBytes{0};
  int64_t bufferedPageFallbackAllocatedBytes{0};
  int64_t bufferedPageWriterDataPageAllocatedBytes{0};
  int64_t bufferedPageWriterDataPageFallbackAllocatedBytes{0};
  int64_t bufferedPageWriterDataPageCount{0};
  int64_t bufferedPageWriterDictionaryPageAllocatedBytes{0};
  int64_t bufferedPageWriterDictionaryPageFallbackAllocatedBytes{0};
  int64_t bufferedPageWriterDictionaryPageCount{0};
  int64_t pageBufferArenaCapacityBytes{0};
  int64_t pageBufferArenaReservedBytes{0};
  int64_t sharedColumnScratchAllocatedBytes{0};
  int64_t columnScratchAllocatedBytes{0};
  int64_t writeContextScratchAllocatedBytes{0};
  int64_t trackedWriterMemoryBytes{0};

  std::string toString() const {
    const auto bufferedPageWriterTotalAllocatedBytes =
        bufferedPageWriterDataPageAllocatedBytes +
        bufferedPageWriterDictionaryPageAllocatedBytes;
    const auto bufferedPageWriterTotalFallbackAllocatedBytes =
        bufferedPageWriterDataPageFallbackAllocatedBytes +
        bufferedPageWriterDictionaryPageFallbackAllocatedBytes;

    std::ostringstream out;
    out << "tracked writer memory: " << formatBytes(trackedWriterMemoryBytes)
        << ", encoder current memory: " << formatBytes(encoderCurrentBytes)
        << ", encoder estimated data encoded size: "
        << formatBytes(encoderEstimatedDataEncodedBytes)
        << ", dict encoder current memory: "
        << formatBytes(dictEncoderCurrentBytes)
        << ", dict encoder estimated data encoded size: "
        << formatBytes(dictEncoderEstimatedDataEncodedBytes)
        << ", buffered page allocated bytes: "
        << formatBytes(bufferedPageAllocatedBytes)
        << ", buffered page actual memory: "
        << formatBytes(bufferedPageActualMemoryBytes)
        << ", buffered page fallback allocated bytes: "
        << formatBytes(bufferedPageFallbackAllocatedBytes)
        << ", buffered page writer total allocated bytes: "
        << formatBytes(bufferedPageWriterTotalAllocatedBytes)
        << ", buffered page writer total fallback allocated bytes: "
        << formatBytes(bufferedPageWriterTotalFallbackAllocatedBytes)
        << ", buffered page writer data page count: "
        << bufferedPageWriterDataPageCount
        << ", buffered page writer data page allocated bytes: "
        << formatBytes(bufferedPageWriterDataPageAllocatedBytes)
        << ", buffered page writer data page fallback allocated bytes: "
        << formatBytes(bufferedPageWriterDataPageFallbackAllocatedBytes)
        << ", buffered page writer dictionary page count: "
        << bufferedPageWriterDictionaryPageCount
        << ", buffered page writer dictionary page allocated bytes: "
        << formatBytes(bufferedPageWriterDictionaryPageAllocatedBytes)
        << ", buffered page writer dictionary page fallback allocated bytes: "
        << formatBytes(bufferedPageWriterDictionaryPageFallbackAllocatedBytes)
        << ", page buffer arena capacity bytes: "
        << formatBytes(pageBufferArenaCapacityBytes)
        << ", page buffer arena reserved bytes: "
        << formatBytes(pageBufferArenaReservedBytes)
        << ", column scratch allocated bytes: "
        << formatBytes(columnScratchAllocatedBytes)
        << ", shared column scratch allocated bytes: "
        << formatBytes(sharedColumnScratchAllocatedBytes)
        << ", write context scratch allocated bytes: "
        << formatBytes(writeContextScratchAllocatedBytes);
    return out.str();
  }

 private:
  static std::string formatBytes(int64_t bytes) {
    if (bytes < 0) {
      const auto magnitude = static_cast<uint64_t>(-(bytes + 1)) + 1;
      return "-" + ::bytedance::bolt::succinctBytes(magnitude);
    }
    return ::bytedance::bolt::succinctBytes(static_cast<uint64_t>(bytes));
  }
};

inline void AddWriterMemoryStats(
    WriterMemoryStats& destination,
    const WriterMemoryStats& source) {
  destination.encoderCurrentBytes += source.encoderCurrentBytes;
  destination.encoderEstimatedDataEncodedBytes +=
      source.encoderEstimatedDataEncodedBytes;
  destination.dictEncoderCurrentBytes += source.dictEncoderCurrentBytes;
  destination.dictEncoderEstimatedDataEncodedBytes +=
      source.dictEncoderEstimatedDataEncodedBytes;
  destination.bufferedPageAllocatedBytes += source.bufferedPageAllocatedBytes;
  destination.bufferedPageActualMemoryBytes +=
      source.bufferedPageActualMemoryBytes;
  destination.bufferedPageFallbackAllocatedBytes +=
      source.bufferedPageFallbackAllocatedBytes;
  destination.bufferedPageWriterDataPageAllocatedBytes +=
      source.bufferedPageWriterDataPageAllocatedBytes;
  destination.bufferedPageWriterDataPageFallbackAllocatedBytes +=
      source.bufferedPageWriterDataPageFallbackAllocatedBytes;
  destination.bufferedPageWriterDataPageCount +=
      source.bufferedPageWriterDataPageCount;
  destination.bufferedPageWriterDictionaryPageAllocatedBytes +=
      source.bufferedPageWriterDictionaryPageAllocatedBytes;
  destination.bufferedPageWriterDictionaryPageFallbackAllocatedBytes +=
      source.bufferedPageWriterDictionaryPageFallbackAllocatedBytes;
  destination.bufferedPageWriterDictionaryPageCount +=
      source.bufferedPageWriterDictionaryPageCount;
  destination.pageBufferArenaCapacityBytes +=
      source.pageBufferArenaCapacityBytes;
  destination.pageBufferArenaReservedBytes +=
      source.pageBufferArenaReservedBytes;
  destination.sharedColumnScratchAllocatedBytes +=
      source.sharedColumnScratchAllocatedBytes;
  destination.columnScratchAllocatedBytes += source.columnScratchAllocatedBytes;
  destination.writeContextScratchAllocatedBytes +=
      source.writeContextScratchAllocatedBytes;
  destination.trackedWriterMemoryBytes += source.trackedWriterMemoryBytes;
}

} // namespace bytedance::bolt::parquet::arrow
