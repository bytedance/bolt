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
#include "bolt/dwio/lance/NativeLanceBatchBuilder.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/dwio/lance/NativeLancePageSource.h"

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

} // namespace

VectorPtr decodeNativeLanceStructuralColumn(
    const NativeLancePageSource& source,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    const NativeLanceMetadata::StructuralField& field,
    uint64_t rowStart,
    uint64_t rowCount) {
  struct Branch {
    uint32_t physicalColumnIndex;
    TypePtr type;
    uint64_t rowsPerParent;
    std::vector<uint32_t> fixedSizeDimensions;
  };
  std::vector<Branch> branches;
  const auto collectBranches =
      [&](const auto& self,
          const NativeLanceMetadata::StructuralField& node) -> void {
    if (node.leaf) {
      branches.push_back(
          {node.physicalColumnIndex, node.type, node.rowsPerParent, {}});
      return;
    }
    for (const auto& child : node.children) {
      const auto first = branches.size();
      self(self, child);
      for (auto index = first; index < branches.size(); ++index) {
        if (node.type->kind() == TypeKind::ROW) {
          branches[index].type =
              ROW({child.name}, {std::move(branches[index].type)});
        } else if (node.type->kind() == TypeKind::MAP) {
          // Arrow maps are encoded structurally as List<Struct<key, value>>.
          branches[index].type = ARRAY(std::move(branches[index].type));
          branches[index].fixedSizeDimensions.insert(
              branches[index].fixedSizeDimensions.begin(), 0);
        } else {
          BOLT_CHECK_EQ(node.type->kind(), TypeKind::ARRAY);
          branches[index].type = ARRAY(std::move(branches[index].type));
          if (child.rowsPerParent != node.rowsPerParent) {
            BOLT_CHECK_EQ(child.rowsPerParent % node.rowsPerParent, 0);
            branches[index].fixedSizeDimensions.insert(
                branches[index].fixedSizeDimensions.begin(),
                child.rowsPerParent / node.rowsPerParent);
          } else {
            branches[index].fixedSizeDimensions.insert(
                branches[index].fixedSizeDimensions.begin(), 0);
          }
        }
      }
    }
  };
  collectBranches(collectBranches, field);
  BOLT_CHECK_EQ(branches.size(), field.physicalColumnCount);

  std::vector<VectorPtr> decodedBranches;
  decodedBranches.reserve(branches.size());
  for (const auto& branch : branches) {
    decodedBranches.push_back(source.decodePhysicalColumn(
        branch.type,
        metadata.physicalColumnLogicalType(branch.physicalColumnIndex),
        branch.physicalColumnIndex,
        rowStart,
        rowCount,
        branch.fixedSizeDimensions));
  }

  const auto checkNulls = [](const BaseVector& expected,
                             const BaseVector& actual) {
    BOLT_CHECK_EQ(expected.size(), actual.size());
    for (vector_size_t row = 0; row < expected.size(); ++row) {
      BOLT_CHECK_EQ(
          expected.isNullAt(row),
          actual.isNullAt(row),
          "Structural sibling validity differs at row {}",
          row);
    }
  };
  const auto merge = [&](const auto& self,
                         const NativeLanceMetadata::StructuralField& node,
                         std::vector<VectorPtr> vectors) -> VectorPtr {
    BOLT_CHECK_EQ(vectors.size(), node.physicalColumnCount);
    if (node.leaf) {
      BOLT_CHECK_EQ(vectors.size(), 1);
      return std::move(vectors.front());
    }
    if (node.type->kind() == TypeKind::ROW) {
      const auto* first = vectors.front()->as<RowVector>();
      BOLT_CHECK_NOT_NULL(first);
      std::vector<VectorPtr> children;
      children.reserve(node.children.size());
      size_t branchIndex = 0;
      for (const auto& child : node.children) {
        std::vector<VectorPtr> childBranches;
        childBranches.reserve(child.physicalColumnCount);
        for (uint32_t i = 0; i < child.physicalColumnCount; ++i) {
          const auto* branch = vectors[branchIndex++]->as<RowVector>();
          BOLT_CHECK_NOT_NULL(branch);
          BOLT_CHECK_EQ(branch->childrenSize(), 1);
          checkNulls(*first, *branch);
          childBranches.push_back(branch->childAt(0));
        }
        children.push_back(self(self, child, std::move(childBranches)));
      }
      BOLT_CHECK_EQ(branchIndex, vectors.size());
      return std::make_shared<RowVector>(
          &pool, node.type, first->nulls(), first->size(), std::move(children));
    }

    BOLT_CHECK(
        node.type->kind() == TypeKind::ARRAY ||
        node.type->kind() == TypeKind::MAP);
    const auto* first = vectors.front()->as<ArrayVector>();
    BOLT_CHECK_NOT_NULL(first);
    std::vector<VectorPtr> elementBranches;
    elementBranches.reserve(vectors.size());
    for (const auto& vector : vectors) {
      const auto* branch = vector->as<ArrayVector>();
      BOLT_CHECK_NOT_NULL(branch);
      checkNulls(*first, *branch);
      for (vector_size_t row = 0; row < first->size(); ++row) {
        BOLT_CHECK_EQ(first->offsetAt(row), branch->offsetAt(row));
        BOLT_CHECK_EQ(first->sizeAt(row), branch->sizeAt(row));
      }
      elementBranches.push_back(branch->elements());
    }
    BOLT_CHECK_EQ(node.children.size(), 1);
    auto elements =
        self(self, node.children.front(), std::move(elementBranches));
    if (node.type->kind() == TypeKind::ARRAY) {
      return std::make_shared<ArrayVector>(
          &pool,
          node.type,
          first->nulls(),
          first->size(),
          first->offsets(),
          first->sizes(),
          std::move(elements));
    }
    const auto* entries = elements->template as<RowVector>();
    BOLT_CHECK_NOT_NULL(entries);
    BOLT_CHECK_EQ(entries->childrenSize(), 2);
    return std::make_shared<MapVector>(
        &pool,
        node.type,
        first->nulls(),
        first->size(),
        first->offsets(),
        first->sizes(),
        entries->childAt(0),
        entries->childAt(1));
  };
  return merge(merge, field, std::move(decodedBranches));
}

namespace {

class FileColumnReader : public NativeLanceColumnReader {
 public:
  FileColumnReader(
      const NativeLanceMetadata& metadata,
      NativeLanceColumnKind kind,
      std::string name,
      TypePtr type,
      uint32_t fileColumnIndex)
      : metadata_(metadata),
        name_(std::move(name)),
        type_(std::move(type)),
        fileColumnIndex_(fileColumnIndex),
        kind_(kind),
        readStage_(
            typeRequiresDeferredRead(type_)
                ? NativeLanceReadStage::kOffsetDependent
                : NativeLanceReadStage::kRowAligned) {}

  bool readFromFile() const override {
    return true;
  }

  NativeLanceColumnKind kind() const override {
    return kind_;
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

  void plan(
      NativeLancePageSource& source,
      const NativeLanceColumnRequest& request) const override {
    source.planColumns({fileColumnIndex_}, request);
  }

  VectorPtr read(
      NativeLancePageSource& source,
      const NativeLanceColumnRequest& request,
      memory::MemoryPool& pool,
      bool rangesPlanned) const override {
    request.validate();
    if (!rangesPlanned) {
      plan(source, request);
    }
    const auto decodeRange = [&](uint64_t rowStart, uint64_t rowCount) {
      if (metadata_.usesStructuralEncoding()) {
        return decodeNativeLanceStructuralColumn(
            source,
            metadata_,
            pool,
            metadata_.structuralField(fileColumnIndex_),
            rowStart,
            rowCount);
      }
      return source.decodePhysicalColumn(
          type_,
          metadata_.columnLogicalType(fileColumnIndex_),
          metadata_.physicalColumnIndex(fileColumnIndex_),
          rowStart,
          rowCount);
    };
    if (request.selection.selectsAll()) {
      return decodeRange(request.rowStart, request.rowCount);
    }
    const auto rows = request.selection.selectedRows();
    if (rows.empty()) {
      return BaseVector::create(type_, 0, &pool);
    }

    auto result = BaseVector::create(type_, rows.size(), &pool);
    size_t outputOffset = 0;
    while (outputOffset < rows.size()) {
      size_t runEnd = outputOffset + 1;
      while (runEnd < rows.size() && rows[runEnd] == rows[runEnd - 1] + 1) {
        ++runEnd;
      }
      const auto values = decodeRange(
          request.rowStart + rows[outputOffset], runEnd - outputOffset);
      result->copy(
          values.get(),
          static_cast<vector_size_t>(outputOffset),
          0,
          static_cast<vector_size_t>(runEnd - outputOffset));
      outputOffset = runEnd;
    }
    return result;
  }

 private:
  const NativeLanceMetadata& metadata_;
  std::string name_;
  TypePtr type_;
  uint32_t fileColumnIndex_;
  NativeLanceColumnKind kind_;
  NativeLanceReadStage readStage_;
};

template <NativeLanceColumnKind readerKind>
class TypedFileColumnReader final : public FileColumnReader {
 public:
  TypedFileColumnReader(
      const NativeLanceMetadata& metadata,
      std::string name,
      TypePtr type,
      uint32_t fileColumnIndex)
      : FileColumnReader(
            metadata,
            readerKind,
            std::move(name),
            std::move(type),
            fileColumnIndex) {}
};

using NativeLanceScalarColumnReader =
    TypedFileColumnReader<NativeLanceColumnKind::kScalar>;
using NativeLanceBinaryColumnReader =
    TypedFileColumnReader<NativeLanceColumnKind::kBinary>;
using NativeLanceDictionaryColumnReader =
    TypedFileColumnReader<NativeLanceColumnKind::kDictionary>;
using NativeLanceListColumnReader =
    TypedFileColumnReader<NativeLanceColumnKind::kList>;
using NativeLanceMapColumnReader =
    TypedFileColumnReader<NativeLanceColumnKind::kMap>;
using NativeLanceStructColumnReader =
    TypedFileColumnReader<NativeLanceColumnKind::kStruct>;
using NativeLanceFixedSizeListColumnReader =
    TypedFileColumnReader<NativeLanceColumnKind::kFixedSizeList>;
using NativeLancePackedStructColumnReader =
    TypedFileColumnReader<NativeLanceColumnKind::kPackedStruct>;
using NativeLanceBlobColumnReader =
    TypedFileColumnReader<NativeLanceColumnKind::kBlob>;

class ConstantColumnReader final : public NativeLanceColumnReader {
 public:
  ConstantColumnReader(std::string name, VectorPtr constantValue)
      : name_(std::move(name)),
        type_(constantValue->type()),
        constantValue_(std::move(constantValue)) {}

  bool readFromFile() const override {
    return false;
  }

  NativeLanceColumnKind kind() const override {
    return NativeLanceColumnKind::kConstant;
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

  void plan(NativeLancePageSource&, const NativeLanceColumnRequest&)
      const override {}

  VectorPtr read(
      NativeLancePageSource&,
      const NativeLanceColumnRequest& request,
      memory::MemoryPool&,
      bool) const override {
    return BaseVector::wrapInConstant(request.outputSize(), 0, constantValue_);
  }

 private:
  std::string name_;
  TypePtr type_;
  VectorPtr constantValue_;
};

NativeLanceColumnKind columnKind(
    const NativeLanceMetadata& metadata,
    uint32_t fileColumnIndex,
    const TypePtr& type) {
  const auto logicalType = metadata.columnLogicalType(fileColumnIndex);
  if (logicalType.rfind("dict:", 0) == 0) {
    return NativeLanceColumnKind::kDictionary;
  }
  if (logicalType == "lance.blob.v2") {
    return NativeLanceColumnKind::kBlob;
  }
  if (logicalType.rfind("fixed_size_list:", 0) == 0) {
    return NativeLanceColumnKind::kFixedSizeList;
  }
  switch (type->kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return NativeLanceColumnKind::kBinary;
    case TypeKind::ARRAY:
      return NativeLanceColumnKind::kList;
    case TypeKind::MAP:
      return NativeLanceColumnKind::kMap;
    case TypeKind::ROW: {
      if (!metadata.usesStructuralEncoding()) {
        const auto physicalIndex =
            metadata.physicalColumnIndex(fileColumnIndex);
        const auto& column = metadata.column(physicalIndex);
        for (int32_t pageIndex = 0; pageIndex < column.pages_size();
             ++pageIndex) {
          if (column.pages(pageIndex).length() == 0) {
            continue;
          }
          const auto& encoding =
              metadata.pageEncoding(physicalIndex, pageIndex);
          if (encoding.array_encoding_case() ==
              ::lance::encodings::ArrayEncoding::kPackedStruct) {
            return NativeLanceColumnKind::kPackedStruct;
          }
          break;
        }
      }
      return NativeLanceColumnKind::kStruct;
    }
    default:
      return NativeLanceColumnKind::kScalar;
  }
}

std::unique_ptr<NativeLanceColumnReader> makeFileColumnReader(
    const NativeLanceMetadata& metadata,
    NativeLanceColumnKind kind,
    std::string name,
    TypePtr type,
    uint32_t fileColumnIndex) {
#define MAKE_READER(readerKind, readerType) \
  case NativeLanceColumnKind::readerKind:   \
    return std::make_unique<readerType>(    \
        metadata, std::move(name), std::move(type), fileColumnIndex)
  switch (kind) {
    MAKE_READER(kScalar, NativeLanceScalarColumnReader);
    MAKE_READER(kBinary, NativeLanceBinaryColumnReader);
    MAKE_READER(kDictionary, NativeLanceDictionaryColumnReader);
    MAKE_READER(kList, NativeLanceListColumnReader);
    MAKE_READER(kMap, NativeLanceMapColumnReader);
    MAKE_READER(kStruct, NativeLanceStructColumnReader);
    MAKE_READER(kFixedSizeList, NativeLanceFixedSizeListColumnReader);
    MAKE_READER(kPackedStruct, NativeLancePackedStructColumnReader);
    MAKE_READER(kBlob, NativeLanceBlobColumnReader);
    case NativeLanceColumnKind::kConstant:
      BOLT_UNREACHABLE();
  }
#undef MAKE_READER
  BOLT_UNREACHABLE();
}

void addProjectedFileColumn(
    const NativeLanceMetadata& metadata,
    const std::string& name,
    std::vector<std::string>& outputNames,
    std::vector<TypePtr>& outputTypes,
    std::vector<std::unique_ptr<NativeLanceColumnReader>>& children) {
  const auto& fileType = metadata.rowType();
  const auto fileColumnIndex = fileType->getChildIdx(name);
  auto type = fileType->childAt(fileColumnIndex);
  const auto kind = columnKind(metadata, fileColumnIndex, type);
  outputNames.push_back(name);
  outputTypes.push_back(type);
  children.push_back(makeFileColumnReader(
      metadata, kind, name, std::move(type), fileColumnIndex));
}

} // namespace

std::unique_ptr<NativeLanceColumnReader>
NativeLanceColumnReader::buildFileColumn(
    const NativeLanceMetadata& metadata,
    uint32_t fileColumnIndex) {
  const auto& fileType = metadata.rowType();
  BOLT_CHECK_LT(fileColumnIndex, fileType->size());
  auto type = fileType->childAt(fileColumnIndex);
  const auto kind = columnKind(metadata, fileColumnIndex, type);
  return makeFileColumnReader(
      metadata,
      kind,
      fileType->nameOf(fileColumnIndex),
      std::move(type),
      fileColumnIndex);
}

NativeLanceRootColumnReader::NativeLanceRootColumnReader(
    RowTypePtr outputType,
    std::vector<std::unique_ptr<NativeLanceColumnReader>> children,
    std::shared_ptr<folly::Executor> decodingExecutor,
    size_t decodingParallelismFactor)
    : outputType_(std::move(outputType)),
      children_(std::move(children)),
      decodingExecutor_(std::move(decodingExecutor)),
      decodingParallelismFactor_(decodingParallelismFactor) {}

std::unique_ptr<NativeLanceRootColumnReader>
NativeLanceRootColumnReader::buildRoot(
    const NativeLanceMetadata& metadata,
    const dwio::common::RowReaderOptions& options) {
  const auto& fileType = metadata.rowType();
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
        const auto kind = columnKind(metadata, fileColumnIndex, type);
        outputTypes[channel] = type;
        children[channel] = makeFileColumnReader(
            metadata,
            kind,
            childSpec->fieldName(),
            std::move(type),
            fileColumnIndex);
      }
    }
    return std::unique_ptr<NativeLanceRootColumnReader>(
        new NativeLanceRootColumnReader(
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
        metadata,
        outputType->nameOf(channel),
        outputNames,
        outputTypes,
        children);
  }
  return std::unique_ptr<NativeLanceRootColumnReader>(
      new NativeLanceRootColumnReader(
          ROW(std::move(outputNames), std::move(outputTypes)),
          std::move(children),
          options.getDecodingExecutor(),
          options.getDecodingParallelismFactor()));
}

std::vector<uint32_t> NativeLanceRootColumnReader::readColumns(
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

VectorPtr NativeLanceRootColumnReader::read(
    NativeLancePageSource& source,
    const NativeLanceColumnRequest& request,
    memory::MemoryPool& pool,
    bool primaryRangesPlanned,
    const std::unordered_map<uint32_t, VectorPtr>* predecodedColumns) const {
  request.validate();
  // A handful of structural or variable-width columns can cost more than a
  // much wider set of scalar columns.  Parallelize whenever at least two
  // compressed columns are available instead of requiring a wide scalar
  // schema.
  constexpr size_t kMinParallelCompressedColumns = 2;
  auto rowAlignedColumns = readColumns(NativeLanceReadStage::kRowAligned);
  auto offsetDependentColumns =
      readColumns(NativeLanceReadStage::kOffsetDependent);
  std::unordered_map<uint32_t, VectorPtr> decodedColumns;
  if (predecodedColumns != nullptr) {
    decodedColumns = *predecodedColumns;
    const auto removePredecoded = [&](std::vector<uint32_t>& columns) {
      columns.erase(
          std::remove_if(
              columns.begin(),
              columns.end(),
              [&](uint32_t column) { return decodedColumns.contains(column); }),
          columns.end());
    };
    removePredecoded(rowAlignedColumns);
    removePredecoded(offsetDependentColumns);
  }
  decodedColumns.reserve(
      decodedColumns.size() + rowAlignedColumns.size() +
      offsetDependentColumns.size());
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

  if (!primaryRangesPlanned && !rowAlignedColumns.empty()) {
    source.planColumns(rowAlignedColumns, request);
  }
  // Submitted streams stay compressed and page-owned until the worker for
  // that column asks for its exact range. ReadScheduler serializes stream
  // extraction while decompression remains parallel across columns.
  const auto rowAlignedCompressed = std::count_if(
      rowAlignedColumns.begin(), rowAlignedColumns.end(), [&](auto column) {
        return source.hasCompressedColumn(
            column, request.rowStart, request.rowCount);
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
            rowAlignedReaders[index]->read(source, request, pool, true);
      });
  for (size_t index = 0; index < rowAlignedColumns.size(); ++index) {
    decodedColumns.emplace(
        rowAlignedColumns[index], std::move(rowAlignedResults[index]));
  }
  if (!primaryRangesPlanned && !offsetDependentColumns.empty()) {
    source.planColumns(offsetDependentColumns, request);
  }
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
        return source.hasCompressedColumn(
            column, request.rowStart, request.rowCount);
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
        offsetDependentResults[index] =
            offsetDependentReaders[index]->read(source, request, pool, true);
      });
  for (size_t index = 0; index < offsetDependentColumns.size(); ++index) {
    decodedColumns.emplace(
        offsetDependentColumns[index],
        std::move(offsetDependentResults[index]));
  }

  NativeLanceBatchBuilder batchBuilder(outputType_, request.outputSize(), pool);
  for (size_t channel = 0; channel < children_.size(); ++channel) {
    const auto& child = children_[channel];
    auto result = child->readFromFile()
        ? decodedColumns.at(child->fileColumnIndex())
        : child->read(source, request, pool, true);
    batchBuilder.setChild(channel, std::move(result));
  }
  return batchBuilder.finish();
}

void NativeLanceRootColumnReader::planRead(
    NativeLancePageSource& source,
    const NativeLanceColumnRequest& request) const {
  request.validate();
  std::vector<uint32_t> columns;
  columns.reserve(children_.size());
  for (const auto& child : children_) {
    if (child->readFromFile()) {
      columns.push_back(child->fileColumnIndex());
    }
  }
  source.planColumns(columns, request);
}

std::vector<uint32_t> NativeLanceRootColumnReader::fileColumnIndices() const {
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

NativeLanceColumnReadTask::NativeLanceColumnReadTask(
    const NativeLanceRootColumnReader& reader,
    NativeLanceColumnRequest request,
    bool primaryRangesPlanned)
    : reader_(reader),
      request_(request),
      state_(
          primaryRangesPlanned ? NativeLanceColumnState::kWaitingPages
                               : NativeLanceColumnState::kIdle) {}

void NativeLanceColumnReadTask::plan(NativeLancePageSource& decoder) {
  if (state_ == NativeLanceColumnState::kFailed) {
    rethrowFailure();
  }
  BOLT_CHECK(
      state_ == NativeLanceColumnState::kIdle,
      "Lance column batch can only be planned from the idle state");
  state_ = NativeLanceColumnState::kPlanningPages;
  try {
    reader_.planRead(decoder, request_);
    state_ = NativeLanceColumnState::kWaitingPages;
  } catch (...) {
    failure_ = std::current_exception();
    state_ = NativeLanceColumnState::kFailed;
    throw;
  }
}

void NativeLanceColumnReadTask::decode(
    NativeLancePageSource& decoder,
    memory::MemoryPool& pool) {
  if (state_ == NativeLanceColumnState::kFailed) {
    rethrowFailure();
  }
  BOLT_CHECK(
      state_ == NativeLanceColumnState::kIdle ||
          state_ == NativeLanceColumnState::kWaitingPages,
      "Lance column batch is not ready to decode");
  const auto primaryRangesPlanned =
      state_ == NativeLanceColumnState::kWaitingPages;
  state_ = NativeLanceColumnState::kAssembling;
  try {
    result_ = reader_.read(decoder, request_, pool, primaryRangesPlanned);
    state_ = NativeLanceColumnState::kReady;
  } catch (...) {
    failure_ = std::current_exception();
    state_ = NativeLanceColumnState::kFailed;
    throw;
  }
}

VectorPtr NativeLanceColumnReadTask::consume() {
  if (state_ == NativeLanceColumnState::kFailed) {
    rethrowFailure();
  }
  BOLT_CHECK(
      state_ == NativeLanceColumnState::kReady,
      "Lance column batch is not ready to consume");
  state_ = NativeLanceColumnState::kConsumed;
  return std::move(result_);
}

void NativeLanceColumnReadTask::cancel(NativeLancePageSource& decoder) {
  if (state_ == NativeLanceColumnState::kConsumed ||
      state_ == NativeLanceColumnState::kFailed ||
      state_ == NativeLanceColumnState::kCancelled) {
    return;
  }
  decoder.cancel();
  result_.reset();
  state_ = NativeLanceColumnState::kCancelled;
}

void NativeLanceColumnReadTask::rethrowFailure() const {
  BOLT_CHECK_NOT_NULL(failure_);
  std::rethrow_exception(failure_);
}

} // namespace bytedance::bolt::lance::reader
