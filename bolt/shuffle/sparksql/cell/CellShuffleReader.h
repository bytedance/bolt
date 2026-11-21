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

#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/base/CompositeShuffleReader.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

class CellNestedAssembler;

/// The Cell shuffle read chain: inherits stream traversal, decompression,
/// metrics, and CompositeRows deserialization from CompositeShuffleReader,
/// and decodes Cell payloads (ShuffleBlockKind::kCellPayload) into batches
/// of at most batchSize rows: a large payload over several batches, small
/// ones into one.
///
/// Batches come out in the logical schema: a nested layout's by a
/// CellNestedAssembler, complex columns packed by the writer restored here by
/// the type adapter.
class CellShuffleReader final : public CompositeShuffleReader {
 public:
  CellShuffleReader(
      std::shared_ptr<ReaderStreamIterator> streams,
      const RowTypePtr& outputType, // logical row type, no pid column
      Codec* codec, // decompression context; may be null (uncompressed)
      arrow::MemoryPool* arrowPool, // only handed to nextStream()
      memory::MemoryPool* pool,
      int32_t batchSize,
      int64_t batchByteSize,
      // The writer's CellShuffleOptions::nativeNestedTypes.
      bool nativeNestedTypes = true);

  ~CellShuffleReader() override;

  std::string_view name() const override {
    return "CellShuffleReader";
  }

 private:
  RowVectorPtr nextNonComposite() override;

  /// Opens the next Cell payload block across streams. Returns false on EOF
  /// or when a CompositeRows block was loaded into the base reader.
  bool nextPayload();

  /// Next batch in the physical (Cell) schema, at most batchSize rows;
  /// nullptr at the end.
  RowVectorPtr nextPhysical();

  /// Built before decoder_ and nestedAssembler_: its schema check covers both.
  const CellShuffleTypeAdapter adapter_;
  CellPayloadDecoder decoder_;
  /// Set for a nested layout, which it reads in place of decoder_.
  std::unique_ptr<CellNestedAssembler> nestedAssembler_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
