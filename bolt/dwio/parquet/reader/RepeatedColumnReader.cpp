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

#include "bolt/dwio/parquet/reader/RepeatedColumnReader.h"
#include "bolt/dwio/parquet/reader/ParquetColumnReader.h"
#include "bolt/dwio/parquet/reader/ParquetReaderCast.h"
#include "bolt/dwio/parquet/reader/StructColumnReader.h"
namespace bytedance::bolt::parquet {

class ParquetTypeWithId;

namespace {
arrow::ValidityBitmapInputOutput prepareRepeatedNulls(
    BufferPtr& nulls,
    int32_t maxItems,
    memory::MemoryPool& memoryPool) {
  dwio::common::ensureCapacity<uint64_t>(
      nulls, bits::nwords(maxItems), &memoryPool);
  arrow::ValidityBitmapInputOutput output;
  output.values_read_upper_bound = maxItems;
  output.values_read = 0;
  output.null_count = 0;
  output.valid_bits = reinterpret_cast<uint8_t*>(nulls->asMutable<uint64_t>());
  output.valid_bits_offset = 0;
  return output;
}

bool isArrayOrMap(const dwio::common::SelectiveColumnReader& reader) {
  const auto kind = reader.fileType().type()->kind();
  return kind == TypeKind::ARRAY || kind == TypeKind::MAP;
}

bool isSupportedFusedStructAndRepeated(
    const arrow::LevelInfo& repeated,
    const arrow::LevelInfo& structure) {
  // Required structs do not need a struct null conversion, so fusing would only
  // add an all-valid bitmap that the old path never produced.
  return structure.def_level > 0 &&
      structure.rep_level + 1 == repeated.rep_level &&
      structure.def_level + 2 == repeated.def_level &&
      structure.repeated_ancestor_def_level ==
      repeated.repeated_ancestor_def_level;
}

void setRepeatedRepDefsFromLeaf(
    dwio::common::SelectiveColumnReader& reader,
    PageReader& pageReader) {
  if (auto* list = dynamic_cast<ListColumnReader*>(&reader)) {
    list->setLengthsFromRepDefs(pageReader);
  } else {
    auto* map = dynamic_cast<MapColumnReader*>(&reader);
    BOLT_CHECK_NOT_NULL(map);
    map->setLengthsFromRepDefs(pageReader);
  }
}

template <typename RepeatedReader>
bool trySetFusedStructAndRepeatedRepDefs(
    PageReader& pageReader,
    StructColumnReader& structReader,
    RepeatedReader& repeatedReader) {
  if (!isSupportedFusedStructAndRepeated(
          repeatedReader.levelInfo(), structReader.levelInfo())) {
    return false;
  }

  const auto repDefRange = pageReader.repDefRange();
  const int32_t numRepDefs = repDefRange.second - repDefRange.first;
  auto lengths = repeatedReader.prepareRepDefLengths(numRepDefs);
  auto repeatedBits = repeatedReader.prepareRepDefNulls(numRepDefs);
  auto structBits = structReader.prepareRepDefNulls(numRepDefs);
  if (!pageReader.getListLengthsAndStructNulls(
          repeatedReader.levelInfo(),
          structReader.levelInfo(),
          repDefRange.first,
          repDefRange.second,
          &repeatedBits,
          lengths->template asMutable<int32_t>(),
          &structBits)) {
    return false;
  }

  repeatedReader.setLengthsFromRepDefOutput(std::move(lengths), repeatedBits);
  structReader.setNullsFromRepDefOutput(structBits);
  return true;
}

bool trySetFusedStructAndRepeatedRepDefs(
    PageReader& pageReader,
    StructColumnReader& structReader,
    dwio::common::SelectiveColumnReader& repDefChild) {
  if (auto* list = dynamic_cast<ListColumnReader*>(&repDefChild)) {
    return trySetFusedStructAndRepeatedRepDefs(pageReader, structReader, *list);
  }

  auto* map = dynamic_cast<MapColumnReader*>(&repDefChild);
  BOLT_CHECK_NOT_NULL(map);
  return trySetFusedStructAndRepeatedRepDefs(pageReader, structReader, *map);
}

PageReader* FOLLY_NULLABLE readLeafRepDefs(
    dwio::common::SelectiveColumnReader* FOLLY_NONNULL reader,
    int32_t numTop,
    bool mustRead,
    bool setRepeatedRepDefs = true) {
  auto children = reader->children();
  if (children.empty()) {
    if (!mustRead) {
      return nullptr;
    }
    auto pageReader = reader->formatData().as<ParquetData>().reader();
    if (pageReader == nullptr) {
      return nullptr;
    }
    pageReader->decodeRepDefs(numTop);
    return pageReader;
  }
  PageReader* pageReader = nullptr;
  auto& type = *reinterpret_cast<const ParquetTypeWithId*>(&reader->fileType());
  if (type.type()->kind() == TypeKind::ARRAY) {
    pageReader = readLeafRepDefs(children[0], numTop, true);
    if (setRepeatedRepDefs) {
      setRepeatedRepDefsFromLeaf(*reader, *pageReader);
    }
    return pageReader;
  }
  if (type.type()->kind() == TypeKind::MAP) {
    pageReader = readLeafRepDefs(children[0], numTop, true);
    readLeafRepDefs(children[1], numTop, false);
    if (setRepeatedRepDefs) {
      setRepeatedRepDefsFromLeaf(*reader, *pageReader);
    }
    return pageReader;
  }
  if (auto structReader = dynamic_cast<StructColumnReader*>(reader)) {
    auto repDefChild = structReader->childForRepDefs();
    if (repDefChild == nullptr) {
      return nullptr;
    }
    const bool fuseStructAndRepeated = isArrayOrMap(*repDefChild);
    pageReader =
        readLeafRepDefs(repDefChild, numTop, true, !fuseStructAndRepeated);
    assert(pageReader);
    if (fuseStructAndRepeated &&
        trySetFusedStructAndRepeatedRepDefs(
            *pageReader, *structReader, *repDefChild)) {
      for (auto i = 0; i < children.size(); ++i) {
        auto child = children[i];
        if (child != repDefChild) {
          readLeafRepDefs(child, numTop, false);
        }
      }
      return pageReader;
    }
    if (fuseStructAndRepeated) {
      setRepeatedRepDefsFromLeaf(*repDefChild, *pageReader);
    }
    structReader->setNullsFromRepDefs(*pageReader);
    for (auto i = 0; i < children.size(); ++i) {
      auto child = children[i];
      if (child != repDefChild) {
        readLeafRepDefs(child, numTop, false);
      }
    }
  }
  return pageReader;
}

void skipUnreadLengthsAndNulls(dwio::common::SelectiveColumnReader& reader) {
  auto children = reader.children();
  if (children.empty()) {
    return;
  }
  if (reader.fileType().type()->kind() == TypeKind::ARRAY) {
    reinterpret_cast<ListColumnReader*>(&reader)->skipUnreadLengths();
  } else if (
      reader.fileType().type()->kind() == TypeKind::ROW ||
      reader.fileType().type()->kind() == TypeKind::VARIANT) {
    reinterpret_cast<StructColumnReader*>(&reader)->seekToEndOfPresetNulls();
  } else if (reader.fileType().type()->kind() == TypeKind::MAP) {
    reinterpret_cast<MapColumnReader*>(&reader)->skipUnreadLengths();
  } else {
    BOLT_UNREACHABLE();
  }
}

void enqueueChildren(
    dwio::common::SelectiveColumnReader* reader,
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto children = reader->children();
  if (children.empty()) {
    return reader->formatData().as<ParquetData>().enqueueRowGroup(index, input);
  }
  for (auto* child : children) {
    enqueueChildren(child, index, input);
  }
}
} // namespace

void ensureRepDefs(
    dwio::common::SelectiveColumnReader& reader,
    int32_t numTop) {
  auto& fileType =
      *reinterpret_cast<const ParquetTypeWithId*>(&reader.fileType());
  // Check that this is a direct child of the root struct.
  if (fileType.parent() && !fileType.parent()->parent()) {
    skipUnreadLengthsAndNulls(reader);
    readLeafRepDefs(&reader, numTop, true);
  }
}

constexpr int64_t kRepDefSkipChunkRows = 4096;

void skipRepDefsInChunks(
    dwio::common::SelectiveColumnReader& reader,
    int64_t numRows) {
  auto& fileType =
      *reinterpret_cast<const ParquetTypeWithId*>(&reader.fileType());
  if (!(fileType.parent() && !fileType.parent()->parent())) {
    return;
  }
  while (numRows > 0) {
    auto chunk =
        static_cast<int32_t>(std::min<int64_t>(numRows, kRepDefSkipChunkRows));
    ensureRepDefs(reader, chunk);
    reader.skip(chunk);
    numRows -= chunk;
  }
}

MapColumnReader::MapColumnReader(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec,
    memory::MemoryPool& pool)
    : dwio::common::SelectiveMapColumnReader(
          requestedType,
          fileType,
          params,
          scanSpec) {
  DWIO_ENSURE_EQ(fileType_->id(), fileType->id(), "working on the same node");
  auto& keyChildType = requestedType->childAt(0);
  auto& elementChildType = requestedType->childAt(1);
  if (params.disableFloatingPointToVarcharMetadataFilter()) {
    if (isReaderCastFilterMismatch(
            fileType_->childAt(0)->type(), requestedType->childAt(0)->type()) ||
        isReaderCastFilterMismatch(
            fileType_->childAt(1)->type(), requestedType->childAt(1)->type())) {
      formatData_->as<ParquetData>().disableTypeDependentMetadataFilters();
    }
  }
  keyReader_ = ParquetColumnReader::build(
      columnReaderOptions,
      keyChildType,
      fileType_->childAt(0),
      params,
      *scanSpec.children()[0],
      pool);
  elementReader_ = ParquetColumnReader::build(
      columnReaderOptions,
      elementChildType,
      fileType_->childAt(1),
      params,
      *scanSpec.children()[1],
      pool);
  reinterpret_cast<const ParquetTypeWithId*>(fileType.get())
      ->makeLevelInfo(levelInfo_);
  children_ = {keyReader_.get(), elementReader_.get()};
}

void MapColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  enqueueChildren(this, index, input);
}

void MapColumnReader::seekToRowGroup(int64_t index) {
  SelectiveMapColumnReader::seekToRowGroup(index);
  readOffset_ = 0;
  childTargetReadOffset_ = 0;
  BufferPtr noBuffer;
  formatData_->as<ParquetData>().setNulls(noBuffer, 0);
  lengths_.setLengths(nullptr);
  keyReader_->seekToRowGroup(index);
  elementReader_->seekToRowGroup(index);
}

void MapColumnReader::skipUnreadLengths() {
  auto& previousLengths = lengths_.lengths();
  if (previousLengths) {
    auto numPreviousLengths =
        (previousLengths->size() / sizeof(vector_size_t)) -
        lengths_.nextLengthIndex();
    if (numPreviousLengths) {
      skip(numPreviousLengths);
    }
  }
}

BufferPtr MapColumnReader::prepareRepDefLengths(int32_t maxItems) {
  BufferPtr lengths = std::move(lengths_.lengths());
  dwio::common::ensureCapacity<int32_t>(lengths, maxItems + 1, &memoryPool_);
  return lengths;
}

arrow::ValidityBitmapInputOutput MapColumnReader::prepareRepDefNulls(
    int32_t maxItems) {
  return prepareRepeatedNulls(nullsInReadRange_, maxItems, memoryPool_);
}

void MapColumnReader::setLengthsFromRepDefOutput(
    BufferPtr lengths,
    const arrow::ValidityBitmapInputOutput& bits) {
  lengths->setSize(bits.values_read * sizeof(int32_t));
  formatData_->as<ParquetData>().setNulls(
      nullsInReadRange(), static_cast<int32_t>(bits.values_read));
  setLengths(std::move(lengths));
}

void MapColumnReader::setLengthsFromRepDefs(PageReader& pageReader) {
  auto repDefRange = pageReader.repDefRange();
  int32_t numRepDefs = repDefRange.second - repDefRange.first;
  BufferPtr lengths = std::move(lengths_.lengths());
  dwio::common::ensureCapacity<int32_t>(lengths, numRepDefs + 1, &memoryPool_);
  memset(lengths->asMutable<uint64_t>(), 0, lengths->size());
  dwio::common::ensureCapacity<uint64_t>(
      nullsInReadRange_, bits::nwords(numRepDefs), &memoryPool_);
  auto numLists = pageReader.getLengthsAndNulls(
      LevelMode::kList,
      levelInfo_,
      repDefRange.first,
      repDefRange.second,
      numRepDefs,
      lengths->asMutable<int32_t>(),
      nullsInReadRange()->asMutable<uint64_t>(),
      0);
  lengths->setSize(numLists * sizeof(int32_t));
  formatData_->as<ParquetData>().setNulls(nullsInReadRange(), numLists);
  setLengths(std::move(lengths));
}

void MapColumnReader::read(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  if (offset > readOffset_) {
    skipRepDefsInChunks(*this, offset - readOffset_);
    readOffset_ = offset;
  }
  // The topmost list reader reads the repdefs for the left subtree.
  ensureRepDefs(*this, rows.back() + 1);
  SelectiveMapColumnReader::read(offset, rows, incomingNulls);

  // The child should be at the end of the range provided to this
  // read() so that it can receive new repdefs for the next set of top
  // level rows. The end of the range is not the end of unused lengths
  // because all lengths maty have been used but the last one might
  // have been 0.  If the last list was 0 and the previous one was not
  // in 'rows' we will be at the end of the last non-zero list in
  // 'rows', which is not the end of the lengths. ORC can seek to this
  // point on next read, Parquet needs to seek here because new
  // repdefs will be scanned and new lengths provided, overwriting the
  // previous ones before the next read().
  keyReader_->seekTo(childTargetReadOffset_, false);
  elementReader_->seekTo(childTargetReadOffset_, false);
}

void MapColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result,
    dwio::common::BufferedInput& input) const {
  // empty placeholder to avoid incorrect calling on parent's impl
  formatData_->filterRowGroups(
      *scanSpec_, rowGroupSize, context, result, input);
}

ListColumnReader::ListColumnReader(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec,
    memory::MemoryPool& pool)
    : dwio::common::SelectiveListColumnReader(
          requestedType,
          fileType,
          params,
          scanSpec) {
  auto& childType = requestedType->childAt(0);
  child_ = ParquetColumnReader::build(
      columnReaderOptions,
      childType,
      fileType_->childAt(0),
      params,
      *scanSpec.children()[0],
      pool);
  reinterpret_cast<const ParquetTypeWithId*>(fileType.get())
      ->makeLevelInfo(levelInfo_);
  children_ = {child_.get()};
}

void ListColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  enqueueChildren(this, index, input);
}

void ListColumnReader::seekToRowGroup(int64_t index) {
  SelectiveListColumnReader::seekToRowGroup(index);
  readOffset_ = 0;
  childTargetReadOffset_ = 0;
  BufferPtr noBuffer;
  formatData_->as<ParquetData>().setNulls(noBuffer, 0);
  lengths_.setLengths(nullptr);
  child_->seekToRowGroup(index);
}

void ListColumnReader::skipUnreadLengths() {
  auto& previousLengths = lengths_.lengths();
  if (previousLengths) {
    auto numPreviousLengths =
        (previousLengths->size() / sizeof(vector_size_t)) -
        lengths_.nextLengthIndex();
    if (numPreviousLengths) {
      skip(numPreviousLengths);
    }
  }
}

BufferPtr ListColumnReader::prepareRepDefLengths(int32_t maxItems) {
  BufferPtr lengths = std::move(lengths_.lengths());
  dwio::common::ensureCapacity<int32_t>(lengths, maxItems + 1, &memoryPool_);
  return lengths;
}

arrow::ValidityBitmapInputOutput ListColumnReader::prepareRepDefNulls(
    int32_t maxItems) {
  return prepareRepeatedNulls(nullsInReadRange_, maxItems, memoryPool_);
}

void ListColumnReader::setLengthsFromRepDefOutput(
    BufferPtr lengths,
    const arrow::ValidityBitmapInputOutput& bits) {
  lengths->setSize(bits.values_read * sizeof(int32_t));
  formatData_->as<ParquetData>().setNulls(
      nullsInReadRange(), static_cast<int32_t>(bits.values_read));
  setLengths(std::move(lengths));
}

void ListColumnReader::setLengthsFromRepDefs(PageReader& pageReader) {
  auto repDefRange = pageReader.repDefRange();
  int32_t numRepDefs = repDefRange.second - repDefRange.first;
  BufferPtr lengths = std::move(lengths_.lengths());
  dwio::common::ensureCapacity<int32_t>(lengths, numRepDefs + 1, &memoryPool_);
  memset(lengths->asMutable<uint64_t>(), 0, lengths->size());
  dwio::common::ensureCapacity<uint64_t>(
      nullsInReadRange_, bits::nwords(numRepDefs), &memoryPool_);
  auto numLists = pageReader.getLengthsAndNulls(
      LevelMode::kList,
      levelInfo_,
      repDefRange.first,
      repDefRange.second,
      numRepDefs,
      lengths->asMutable<int32_t>(),
      nullsInReadRange()->asMutable<uint64_t>(),
      0);
  lengths->setSize(numLists * sizeof(int32_t));
  formatData_->as<ParquetData>().setNulls(nullsInReadRange(), numLists);
  setLengths(std::move(lengths));
}
void ListColumnReader::read(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  if (offset > readOffset_) {
    skipRepDefsInChunks(*this, offset - readOffset_);
    readOffset_ = offset;
  }
  // The topmost list reader reads the repdefs for the left subtree.
  ensureRepDefs(*this, rows.back() + 1);
  SelectiveListColumnReader::read(offset, rows, incomingNulls);

  // The child should be at the end of the range provided to this
  // read() so that it can receive new repdefs for the next set of top
  // level rows. The end of the range is not the end of unused lengths
  // because all lengths maty have been used but the last one might
  // have been 0.  If the last list was 0 and the previous one was not
  // in 'rows' we will be at the end of the last non-zero list in
  // 'rows', which is not the end of the lengths. ORC can seek to this
  // point on next read, Parquet needs to seek here because new
  // repdefs will be scanned and new lengths provided, overwriting the
  // previous ones before the next read().
  child_->seekTo(childTargetReadOffset_, false);
}

void ListColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result,
    dwio::common::BufferedInput& input) const {
  // empty placeholder to avoid incorrect calling on parent's impl
}

} // namespace bytedance::bolt::parquet
