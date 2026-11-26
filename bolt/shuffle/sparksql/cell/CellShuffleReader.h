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

#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/base/CompositeShuffleReader.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

class CellNestedAssembler;

/// The Cell shuffle read chain: inherits stream traversal, decompression,
/// metrics, and CompositeRows deserialization from CompositeShuffleReader,
/// and decodes Cell payloads (ShuffleBlockKind::kCellPayload) into batches
/// of at most maxBatchRows rows: a large payload over several batches, small
/// ones into one.
///
/// Batches come out in the logical schema: a nested layout's by a
/// CellNestedAssembler, complex columns packed by the writer restored here by
/// the type adapter.
class CellShuffleReader final : public CompositeShuffleReader {
 public:
  /// Creates a CellShuffleReader from ShuffleReaderOptions, owning its
  /// decompression codec (null when compressionType is UNCOMPRESSED).
  CellShuffleReader(
      std::shared_ptr<ReaderStreamIterator> streams,
      const RowTypePtr& outputType, // logical row type, no pid column
      const ShuffleReaderOptions& options,
      arrow::MemoryPool* arrowPool, // only handed to nextStream()
      memory::MemoryPool* pool);

  CellShuffleReader(
      std::shared_ptr<ReaderStreamIterator> streams,
      const RowTypePtr& outputType, // logical row type, no pid column
      Codec* codec, // decompression context; may be null (uncompressed)
      arrow::MemoryPool* arrowPool, // only handed to nextStream()
      memory::MemoryPool* pool,
      int32_t maxBatchRows,
      int64_t maxBatchBytes,
      // The writer's CellShuffleOptions::nativeNestedTypes.
      bool nativeNestedTypes = true);

  ~CellShuffleReader() override;

  std::string_view name() const override {
    return "CellShuffleReader";
  }

 private:
  void init(const RowTypePtr& outputType, bool nativeNestedTypes);
  RowVectorPtr nextNonComposite() override;

  /// Opens the next Cell payload block across streams. Returns false on EOF
  /// or when a CompositeRows block was loaded into the base reader.
  bool nextPayload();

  /// Next batch in the physical (Cell) schema, at most maxBatchRows rows;
  /// nullptr at the end.
  RowVectorPtr nextPhysical();

  /// Set for flat/adapted layouts; null when nestedAssembler_ handles the
  /// schema natively.
  std::unique_ptr<CellShuffleTypeAdapter> adapter_;
  std::unique_ptr<CellPayloadDecoder> decoder_;
  /// Set for a nested layout, mutually exclusive with adapter_/decoder_.
  std::unique_ptr<CellNestedAssembler> nestedAssembler_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
