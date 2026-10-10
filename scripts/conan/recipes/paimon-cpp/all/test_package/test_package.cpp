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

#include <arrow/c/abi.h>
#include <arrow/memory_pool.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/reader/file_batch_reader.h>
#include <array>
#include <iostream>

#ifdef PAIMON_HAS_ROW_MAPPING
// This exported bridge has an internal header that is not installed. Declare
// its ABI here to exercise the Arrow 15 compatibility patch in the package.
namespace paimon {
std::unique_ptr<arrow::MemoryPool> GetArrowPool(
    const std::shared_ptr<MemoryPool>& pool);
}

bool checkMemoryAccounting() {
  auto pool = paimon::GetArrowPool(paimon::GetDefaultPool());
  uint8_t* buffer = nullptr;
  if (!pool->Allocate(64, 64, &buffer).ok()) {
    return false;
  }
  bool valid = pool->bytes_allocated() == 64 && pool->num_allocations() == 1;
  if (!pool->Reallocate(64, 128, 64, &buffer).ok()) {
    pool->Free(buffer, 64, 64);
    return false;
  }
  valid &= pool->bytes_allocated() == 128 && pool->num_allocations() == 2;
  if (!pool->Reallocate(128, 128, 64, &buffer).ok()) {
    pool->Free(buffer, 128, 64);
    return false;
  }
  valid &= pool->bytes_allocated() == 128 && pool->num_allocations() == 2;
  if (!pool->Reallocate(128, 32, 64, &buffer).ok()) {
    pool->Free(buffer, 128, 64);
    return false;
  }
  valid &= pool->bytes_allocated() == 32 && pool->num_allocations() == 3;
  pool->Free(buffer, 32, 64);
  return valid && pool->bytes_allocated() == 0 && pool->max_memory() == 128 &&
      pool->total_bytes_allocated() == 128 && pool->num_allocations() == 3;
}
#endif

// Exercise the installed interface and the core library's default bitmap
// implementation with compacted rows whose physical positions have holes.
class MappedReader final : public paimon::FileBatchReader {
 public:
  paimon::Result<std::unique_ptr<ArrowSchema>> GetFileSchema() const override {
    return std::make_unique<ArrowSchema>();
  }

  paimon::Status SetReadSchema(
      ArrowSchema*,
      const std::shared_ptr<paimon::Predicate>&,
      const std::optional<paimon::RoaringBitmap32>&) override {
    read_ = false;
    return paimon::Status::OK();
  }

  paimon::Result<ReadBatch> NextBatch() override {
    if (read_) {
      eof_ = true;
      return MakeEofBatch();
    }
    read_ = true;
    auto array = std::make_unique<ArrowArray>();
    array->length = positions_.size();
    array->release = [](ArrowArray* value) { value->release = nullptr; };
    auto schema = std::make_unique<ArrowSchema>();
    schema->format = "+s";
    schema->release = [](ArrowSchema* value) { value->release = nullptr; };
    return std::make_pair(std::move(array), std::move(schema));
  }

#ifdef PAIMON_HAS_ROW_MAPPING
  paimon::Result<uint64_t> GetPreviousBatchFileRowId(
      uint64_t index) const override {
    if (!read_ || eof_ || index >= positions_.size()) {
      return paimon::Status::Invalid("No row at this batch index");
    }
    return positions_[index];
  }
#else
  paimon::Result<uint64_t> GetPreviousBatchFirstRowNumber() const override {
    return positions_[0];
  }
#endif

  paimon::Result<uint64_t> GetNumberOfRows() const override {
    return uint64_t{9};
  }
  bool SupportPreciseBitmapSelection() const override {
    return false;
  }
  std::shared_ptr<paimon::Metrics> GetReaderMetrics() const override {
    return nullptr;
  }
  void Close() override {}

 private:
  const std::array<uint64_t, 3> positions_{2, 5, 8};
  bool read_{false};
  bool eof_{false};
};

int main() {
#ifdef PAIMON_HAS_ROW_MAPPING
  if (!checkMemoryAccounting()) {
    std::cerr << "Arrow bridge memory accounting failed" << std::endl;
    return 6;
  }
#endif
  MappedReader reader;
  auto result = reader.NextBatchWithBitmap();
  if (!result.ok()) {
    std::cerr << result.status().ToString() << std::endl;
    return 1;
  }
  auto batch = std::move(result).value();
  if (!(batch.second == paimon::RoaringBitmap32::From({0, 1, 2}))) {
    return 2;
  }
#ifdef PAIMON_HAS_ROW_MAPPING
  paimon::FileBatchReader& interface = reader;
  for (uint64_t i = 0; i < 3; ++i) {
    auto position = interface.GetPreviousBatchFileRowId(i);
    if (!position.ok() || position.value() != 2 + 3 * i) {
      return 3;
    }
  }
  if (interface.GetPreviousBatchFileRowId(3).ok()) {
    return 4;
  }
#endif
  batch.first.first->release(batch.first.first.get());
  batch.first.second->release(batch.first.second.get());
  auto eof = reader.NextBatchWithBitmap();
  if (!eof.ok() || !paimon::BatchReader::IsEofBatch(eof.value())) {
    return 5;
  }
  std::cout << "Paimon consumer and batch bitmap interface passed" << std::endl;
  return 0;
}
