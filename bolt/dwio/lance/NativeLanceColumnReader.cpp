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

#include "bolt/dwio/lance/NativeLanceColumnReader.h"

#include <algorithm>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {
namespace {

bool typeRequiresDeferredRead(const TypePtr& type) {
  if (type->kind() == TypeKind::VARCHAR ||
      type->kind() == TypeKind::VARBINARY || type->kind() == TypeKind::ARRAY ||
      type->kind() == TypeKind::MAP) {
    return true;
  }
  if (type->kind() == TypeKind::ROW) {
    for (uint32_t i = 0; i < type->size(); ++i) {
      if (typeRequiresDeferredRead(type->childAt(i))) {
        return true;
      }
    }
  }
  return false;
}

class FileColumnReader final : public NativeLanceColumnReader {
 public:
  FileColumnReader(std::string name, TypePtr type, uint32_t fileColumnIndex)
      : name_(std::move(name)),
        type_(std::move(type)),
        fileColumnIndex_(fileColumnIndex),
        readStage_(
            typeRequiresDeferredRead(type_)
                ? NativeLanceReadStage::kOffsetDependent
                : NativeLanceReadStage::kRowAligned) {}

  bool readFromFile() const override {
    return true;
  }

  NativeLanceReadStage readStage() const override {
    return readStage_;
  }

  uint32_t fileColumnIndex() const override {
    return fileColumnIndex_;
  }

  const TypePtr& type() const override {
    return type_;
  }

  VectorPtr read(
      NativeLanceDecoder& decoder,
      uint64_t rowStart,
      uint64_t rowCount,
      memory::MemoryPool&) const override {
    return decoder.decodeColumn(fileColumnIndex_, rowStart, rowCount);
  }

 private:
  std::string name_;
  TypePtr type_;
  uint32_t fileColumnIndex_;
  NativeLanceReadStage readStage_;
};

class ConstantColumnReader final : public NativeLanceColumnReader {
 public:
  ConstantColumnReader(std::string name, VectorPtr constantValue)
      : name_(std::move(name)),
        type_(constantValue->type()),
        constantValue_(std::move(constantValue)) {}

  bool readFromFile() const override {
    return false;
  }

  NativeLanceReadStage readStage() const override {
    return NativeLanceReadStage::kRowAligned;
  }

  uint32_t fileColumnIndex() const override {
    BOLT_FAIL("Constant Lance column has no file column index");
  }

  const TypePtr& type() const override {
    return type_;
  }

  VectorPtr read(
      NativeLanceDecoder&,
      uint64_t,
      uint64_t rowCount,
      memory::MemoryPool&) const override {
    return BaseVector::wrapInConstant(rowCount, 0, constantValue_);
  }

 private:
  std::string name_;
  TypePtr type_;
  VectorPtr constantValue_;
};

void addProjectedFileColumn(
    const RowTypePtr& fileType,
    const std::string& name,
    std::vector<std::string>& outputNames,
    std::vector<TypePtr>& outputTypes,
    std::vector<std::unique_ptr<NativeLanceColumnReader>>& children) {
  const auto fileColumnIndex = fileType->getChildIdx(name);
  auto type = fileType->childAt(fileColumnIndex);
  outputNames.push_back(name);
  outputTypes.push_back(type);
  children.push_back(std::make_unique<FileColumnReader>(
      name, std::move(type), fileColumnIndex));
}

} // namespace

NativeLanceStructColumnReader::NativeLanceStructColumnReader(
    RowTypePtr outputType,
    std::vector<std::unique_ptr<NativeLanceColumnReader>> children,
    std::shared_ptr<folly::Executor> decodingExecutor,
    size_t decodingParallelismFactor)
    : outputType_(std::move(outputType)),
      children_(std::move(children)),
      decodingExecutor_(std::move(decodingExecutor)),
      decodingParallelismFactor_(decodingParallelismFactor) {}

std::unique_ptr<NativeLanceStructColumnReader>
NativeLanceStructColumnReader::buildRoot(
    const RowTypePtr& fileType,
    const dwio::common::RowReaderOptions& options) {
  std::vector<std::string> outputNames;
  std::vector<TypePtr> outputTypes;
  std::vector<std::unique_ptr<NativeLanceColumnReader>> children;

  if (const auto& scanSpec = options.getScanSpec()) {
    column_index_t numOutputColumns = 0;
    for (const auto& childSpec : scanSpec->children()) {
      if (childSpec->projectOut()) {
        BOLT_CHECK_NE(childSpec->channel(), common::ScanSpec::kNoChannel);
        numOutputColumns = std::max(numOutputColumns, childSpec->channel() + 1);
      }
    }
    outputNames.resize(numOutputColumns);
    outputTypes.resize(numOutputColumns);
    children.resize(numOutputColumns);
    for (const auto& childSpec : scanSpec->children()) {
      if (!childSpec->projectOut()) {
        continue;
      }
      const auto channel = childSpec->channel();
      outputNames[channel] = childSpec->fieldName();
      if (childSpec->isConstant()) {
        outputTypes[channel] = childSpec->constantValue()->type();
        children[channel] = std::make_unique<ConstantColumnReader>(
            childSpec->fieldName(), childSpec->constantValue());
      } else {
        const auto fileColumnIndex =
            fileType->getChildIdx(childSpec->fieldName());
        auto type = fileType->childAt(fileColumnIndex);
        outputTypes[channel] = type;
        children[channel] = std::make_unique<FileColumnReader>(
            childSpec->fieldName(), std::move(type), fileColumnIndex);
      }
    }
    return std::unique_ptr<NativeLanceStructColumnReader>(
        new NativeLanceStructColumnReader(
            ROW(std::move(outputNames), std::move(outputTypes)),
            std::move(children),
            options.getDecodingExecutor(),
            options.getDecodingParallelismFactor()));
  }

  RowTypePtr outputType;
  if (const auto& selector = options.getSelector()) {
    outputType = selector->buildSelectedReordered();
  } else {
    outputType = fileType;
  }
  outputNames.reserve(outputType->size());
  outputTypes.reserve(outputType->size());
  children.reserve(outputType->size());
  for (uint32_t channel = 0; channel < outputType->size(); ++channel) {
    addProjectedFileColumn(
        fileType,
        outputType->nameOf(channel),
        outputNames,
        outputTypes,
        children);
  }
  return std::unique_ptr<NativeLanceStructColumnReader>(
      new NativeLanceStructColumnReader(
          ROW(std::move(outputNames), std::move(outputTypes)),
          std::move(children),
          options.getDecodingExecutor(),
          options.getDecodingParallelismFactor()));
}

std::vector<uint32_t> NativeLanceStructColumnReader::readColumns(
    NativeLanceReadStage stage) const {
  std::vector<uint32_t> columns;
  columns.reserve(children_.size());
  for (const auto& child : children_) {
    if (child->readFromFile() && child->readStage() == stage) {
      columns.push_back(child->fileColumnIndex());
    }
  }
  std::sort(columns.begin(), columns.end());
  columns.erase(std::unique(columns.begin(), columns.end()), columns.end());
  return columns;
}

VectorPtr NativeLanceStructColumnReader::read(
    NativeLanceDecoder& decoder,
    uint64_t rowStart,
    uint64_t rowCount,
    memory::MemoryPool& pool) const {
  constexpr size_t kMinParallelCompressedColumns = 16;
  const auto rowAlignedColumns = readColumns(NativeLanceReadStage::kRowAligned);
  const auto offsetDependentColumns =
      readColumns(NativeLanceReadStage::kOffsetDependent);
  std::unordered_map<uint32_t, VectorPtr> decodedColumns;
  decodedColumns.reserve(
      rowAlignedColumns.size() + offsetDependentColumns.size());
  const auto readerForColumn = [&](uint32_t column) {
    const auto reader = std::find_if(
        children_.begin(), children_.end(), [&](const auto& child) {
          return child->readFromFile() && child->fileColumnIndex() == column;
        });
    BOLT_CHECK(reader != children_.end());
    return reader->get();
  };
  std::vector<const NativeLanceColumnReader*> rowAlignedReaders;
  rowAlignedReaders.reserve(rowAlignedColumns.size());
  for (const auto column : rowAlignedColumns) {
    rowAlignedReaders.push_back(readerForColumn(column));
  }
  std::vector<VectorPtr> rowAlignedResults(rowAlignedColumns.size());

  decoder.planColumns(rowAlignedColumns, rowStart, rowCount);
  // Finish I/O before worker threads enter the decoder. Each worker then
  // consumes independent buffers and can decompress its column concurrently.
  decoder.materializeReadPlan();
  const auto rowAlignedCompressed = std::count_if(
      rowAlignedColumns.begin(), rowAlignedColumns.end(), [&](auto column) {
        return decoder.hasCompressedColumn(column, rowStart, rowCount);
      });
  dwio::common::ParallelFor(
      pool.threadSafe() && rowAlignedCompressed >= kMinParallelCompressedColumns
          ? decodingExecutor_
          : nullptr,
      0,
      rowAlignedColumns.size(),
      decodingParallelismFactor_)
      .execute([&](size_t index) {
        rowAlignedResults[index] =
            rowAlignedReaders[index]->read(decoder, rowStart, rowCount, pool);
      });
  for (size_t index = 0; index < rowAlignedColumns.size(); ++index) {
    decodedColumns.emplace(
        rowAlignedColumns[index], std::move(rowAlignedResults[index]));
  }
  decoder.planColumns(offsetDependentColumns, rowStart, rowCount);
  decoder.materializeReadPlan();
  std::vector<const NativeLanceColumnReader*> offsetDependentReaders;
  offsetDependentReaders.reserve(offsetDependentColumns.size());
  for (const auto column : offsetDependentColumns) {
    offsetDependentReaders.push_back(readerForColumn(column));
  }
  std::vector<VectorPtr> offsetDependentResults(offsetDependentColumns.size());
  const auto offsetDependentCompressed = std::count_if(
      offsetDependentColumns.begin(),
      offsetDependentColumns.end(),
      [&](auto column) {
        return decoder.hasCompressedColumn(column, rowStart, rowCount);
      });
  dwio::common::ParallelFor(
      pool.threadSafe() &&
              offsetDependentCompressed >= kMinParallelCompressedColumns
          ? decodingExecutor_
          : nullptr,
      0,
      offsetDependentColumns.size(),
      decodingParallelismFactor_)
      .execute([&](size_t index) {
        offsetDependentResults[index] = offsetDependentReaders[index]->read(
            decoder, rowStart, rowCount, pool);
      });
  for (size_t index = 0; index < offsetDependentColumns.size(); ++index) {
    decodedColumns.emplace(
        offsetDependentColumns[index],
        std::move(offsetDependentResults[index]));
  }

  std::vector<VectorPtr> children(children_.size());
  for (size_t channel = 0; channel < children_.size(); ++channel) {
    const auto& child = children_[channel];
    children[channel] = child->readFromFile()
        ? decodedColumns.at(child->fileColumnIndex())
        : child->read(decoder, rowStart, rowCount, pool);
  }
  return std::make_shared<RowVector>(
      &pool,
      outputType_,
      nullptr,
      static_cast<vector_size_t>(rowCount),
      std::move(children));
}

void NativeLanceStructColumnReader::planRead(
    NativeLanceDecoder& decoder,
    uint64_t rowStart,
    uint64_t rowCount) const {
  std::vector<uint32_t> columns;
  columns.reserve(children_.size());
  for (const auto& child : children_) {
    if (child->readFromFile()) {
      columns.push_back(child->fileColumnIndex());
    }
  }
  decoder.planColumns(columns, rowStart, rowCount);
}

std::vector<uint32_t> NativeLanceStructColumnReader::fileColumnIndices() const {
  std::vector<uint32_t> columns;
  columns.reserve(children_.size());
  for (const auto& child : children_) {
    if (child->readFromFile()) {
      columns.push_back(child->fileColumnIndex());
    }
  }
  std::sort(columns.begin(), columns.end());
  columns.erase(std::unique(columns.begin(), columns.end()), columns.end());
  return columns;
}

} // namespace bytedance::bolt::lance::reader
