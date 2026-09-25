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
#include <cstring>
#include <limits>

#include <folly/ScopeGuard.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/dwio/lance/NativeLancePageSource.h"

namespace bytedance::bolt::lance::reader {

uint64_t estimateNativeLanceTypeBytesPerRow(const TypePtr& type) {
  constexpr uint64_t kNullOverhead = 1;
  switch (type->kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return sizeof(StringView) + 16 + kNullOverhead;
    case TypeKind::ARRAY:
      return 2 * sizeof(vector_size_t) +
          estimateNativeLanceTypeBytesPerRow(type->childAt(0)) + kNullOverhead;
    case TypeKind::MAP:
      return 2 * sizeof(vector_size_t) +
          estimateNativeLanceTypeBytesPerRow(type->childAt(0)) +
          estimateNativeLanceTypeBytesPerRow(type->childAt(1)) + kNullOverhead;
    case TypeKind::ROW: {
      uint64_t size = kNullOverhead;
      for (uint32_t i = 0; i < type->size(); ++i) {
        size += estimateNativeLanceTypeBytesPerRow(type->childAt(i));
      }
      return size;
    }
    case TypeKind::UNKNOWN:
      return kNullOverhead;
    default:
      return type->cppSizeInBytes() + kNullOverhead;
  }
}

bool nativeLanceTypeRequiresDeferredRead(const TypePtr& type) {
  if (type->kind() == TypeKind::VARCHAR ||
      type->kind() == TypeKind::VARBINARY || type->kind() == TypeKind::ARRAY ||
      type->kind() == TypeKind::MAP) {
    return true;
  }
  if (type->kind() == TypeKind::ROW) {
    for (uint32_t i = 0; i < type->size(); ++i) {
      if (nativeLanceTypeRequiresDeferredRead(type->childAt(i))) {
        return true;
      }
    }
  }
  return false;
}

namespace {

void checkMatchingNulls(const BaseVector& expected, const BaseVector& actual) {
  BOLT_CHECK_EQ(expected.size(), actual.size());
  if (expected.rawNulls() == actual.rawNulls()) {
    return;
  }
  const auto words = bits::nwords(expected.size());
  for (uint64_t word = 0; word < words; ++word) {
    const auto expectedBits = expected.rawNulls() == nullptr
        ? bits::kNotNull64
        : expected.rawNulls()[word];
    const auto actualBits = actual.rawNulls() == nullptr
        ? bits::kNotNull64
        : actual.rawNulls()[word];
    const auto remaining = expected.size() - word * 64;
    const auto mask = remaining < 64 ? bits::lowMask(remaining) : ~uint64_t{0};
    BOLT_CHECK_EQ(
        (expectedBits ^ actualBits) & mask,
        0,
        "Structural sibling validity differs in word {}",
        word);
  }
}

void checkMatchingArrayLayout(
    const ArrayVectorBase& expected,
    const ArrayVectorBase& actual) {
  checkMatchingNulls(expected, actual);
  const auto bytes = expected.size() * sizeof(vector_size_t);
  BOLT_CHECK_EQ(
      std::memcmp(expected.rawOffsets(), actual.rawOffsets(), bytes),
      0,
      "Structural sibling offsets differ");
  BOLT_CHECK_EQ(
      std::memcmp(expected.rawSizes(), actual.rawSizes(), bytes),
      0,
      "Structural sibling sizes differ");
}

} // namespace

VectorPtr decodeNativeLanceStructuralColumn(
    const NativeLancePageSource& source,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    const NativeLanceMetadata::StructuralField& field,
    uint64_t rowStart,
    uint64_t rowCount,
    NativeLanceDecoderStateRetention decoderStateRetention) {
  struct Branch {
    uint32_t physicalColumnIndex;
    TypePtr type;
    uint64_t rowsPerParent;
    std::vector<uint32_t> fixedSizeDimensions;
    std::vector<const NativeLanceMetadata::StructuralField*> shapePath;
  };
  std::vector<Branch> branches;
  const auto collectBranches =
      [&](const auto& self,
          const NativeLanceMetadata::StructuralField& node) -> void {
    if (node.leaf) {
      branches.push_back(
          {node.physicalColumnIndex, node.type, node.rowsPerParent, {}, {}});
      return;
    }
    for (const auto& child : node.children) {
      const auto first = branches.size();
      self(self, child);
      for (auto index = first; index < branches.size(); ++index) {
        branches[index].shapePath.insert(
            branches[index].shapePath.begin(), &node);
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
  std::unordered_map<const NativeLanceMetadata::StructuralField*, VectorPtr>
      canonicalShapes;
  // Each physical leaf carries the same ancestor shape. Rebind sibling
  // wrappers to the first decoded buffers so duplicate null/offset/size
  // storage is released before the remaining leaves are decoded.
  const auto shareShape = [&](const auto& self,
                              const Branch& branch,
                              size_t depth,
                              VectorPtr vector) -> VectorPtr {
    if (depth == branch.shapePath.size()) {
      return vector;
    }
    const auto* node = branch.shapePath[depth];
    if (node->type->kind() == TypeKind::ROW) {
      const auto* row = vector->as<RowVector>();
      BOLT_CHECK_NOT_NULL(row);
      BOLT_CHECK_EQ(row->childrenSize(), 1);
      auto child = self(self, branch, depth + 1, row->childAt(0));
      const auto [shape, inserted] = canonicalShapes.try_emplace(node, vector);
      if (inserted) {
        return vector;
      }
      const auto* canonical = shape->second->as<RowVector>();
      BOLT_CHECK_NOT_NULL(canonical);
      checkMatchingNulls(*canonical, *row);
      return std::make_shared<RowVector>(
          &pool,
          vector->type(),
          canonical->nulls(),
          canonical->size(),
          std::vector<VectorPtr>{std::move(child)});
    }

    BOLT_CHECK(
        node->type->kind() == TypeKind::ARRAY ||
        node->type->kind() == TypeKind::MAP);
    const auto* array = vector->as<ArrayVector>();
    BOLT_CHECK_NOT_NULL(array);
    auto child = self(self, branch, depth + 1, array->elements());
    const auto [shape, inserted] = canonicalShapes.try_emplace(node, vector);
    if (inserted) {
      return vector;
    }
    const auto* canonical = shape->second->as<ArrayVector>();
    BOLT_CHECK_NOT_NULL(canonical);
    checkMatchingArrayLayout(*canonical, *array);
    return std::make_shared<ArrayVector>(
        &pool,
        vector->type(),
        canonical->nulls(),
        canonical->size(),
        canonical->offsets(),
        canonical->sizes(),
        std::move(child));
  };
  for (const auto& branch : branches) {
    auto decoded = source.decodePhysicalColumn(
        branch.type,
        metadata.physicalColumnLogicalType(branch.physicalColumnIndex),
        branch.physicalColumnIndex,
        rowStart,
        rowCount,
        branch.fixedSizeDimensions,
        decoderStateRetention);
    decodedBranches.push_back(
        shareShape(shareShape, branch, 0, std::move(decoded)));
  }

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
          checkMatchingNulls(*first, *branch);
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
      checkMatchingArrayLayout(*first, *branch);
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

class FileColumnReader final : public NativeLanceColumnReader {
 public:
  FileColumnReader(
      const NativeLanceMetadata& metadata,
      TypePtr type,
      uint32_t fileColumnIndex)
      : metadata_(metadata),
        type_(std::move(type)),
        fileColumnIndex_(fileColumnIndex),
        readStage_(
            nativeLanceTypeRequiresDeferredRead(type_)
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
    const auto releasePageState = folly::makeGuard([&]() {
      if (request.decoderStateRetention ==
          NativeLanceDecoderStateRetention::kRequest) {
        source.releaseLegacyPageReadersForLogicalColumn(fileColumnIndex_);
      }
    });
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
            rowCount,
            request.decoderStateRetention);
      }
      return source.decodePhysicalColumn(
          type_,
          metadata_.columnLogicalType(fileColumnIndex_),
          metadata_.physicalColumnIndex(fileColumnIndex_),
          rowStart,
          rowCount,
          {},
          request.decoderStateRetention);
    };
    if (request.selection.selectsAll()) {
      return decodeRange(request.rowStart, request.rowCount);
    }
    const auto rows = request.selection.selectedRows();
    if (rows.empty()) {
      return BaseVector::create(type_, 0, &pool);
    }

    auto result = BaseVector::create(type_, rows.size(), &pool);
    vector_size_t outputOffset = 0;
    for (const auto& range : request.selection.selectedRanges()) {
      const auto values =
          decodeRange(request.rowStart + range.begin, range.size);
      result->copy(values.get(), outputOffset, 0, range.size);
      outputOffset += range.size;
    }
    BOLT_CHECK_EQ(outputOffset, result->size());
    return result;
  }

 private:
  const NativeLanceMetadata& metadata_;
  TypePtr type_;
  uint32_t fileColumnIndex_;
  NativeLanceReadStage readStage_;
};

class ConstantColumnReader final : public NativeLanceColumnReader {
 public:
  explicit ConstantColumnReader(VectorPtr constantValue)
      : type_(constantValue->type()),
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
  TypePtr type_;
  VectorPtr constantValue_;
};

std::unique_ptr<NativeLanceColumnReader> makeFileColumnReader(
    const NativeLanceMetadata& metadata,
    TypePtr type,
    uint32_t fileColumnIndex) {
  return std::make_unique<FileColumnReader>(
      metadata, std::move(type), fileColumnIndex);
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
  outputNames.push_back(name);
  outputTypes.push_back(type);
  children.push_back(
      makeFileColumnReader(metadata, std::move(type), fileColumnIndex));
}

} // namespace

std::unique_ptr<NativeLanceColumnReader>
NativeLanceColumnReader::buildFileColumn(
    const NativeLanceMetadata& metadata,
    uint32_t fileColumnIndex) {
  const auto& fileType = metadata.rowType();
  BOLT_CHECK_LT(fileColumnIndex, fileType->size());
  auto type = fileType->childAt(fileColumnIndex);
  return makeFileColumnReader(metadata, std::move(type), fileColumnIndex);
}

NativeLanceRootColumnReader::NativeLanceRootColumnReader(
    RowTypePtr outputType,
    std::vector<std::unique_ptr<NativeLanceColumnReader>> children,
    std::shared_ptr<folly::Executor> decodingExecutor,
    size_t decodingParallelismFactor)
    : outputType_(std::move(outputType)),
      children_(std::move(children)),
      decodingExecutor_(std::move(decodingExecutor)),
      decodingParallelismFactor_(decodingParallelismFactor) {
  initializeReadPlan();
}

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
        children[channel] =
            std::make_unique<ConstantColumnReader>(childSpec->constantValue());
      } else {
        const auto fileColumnIndex =
            fileType->getChildIdx(childSpec->fieldName());
        auto type = fileType->childAt(fileColumnIndex);
        outputTypes[channel] = type;
        children[channel] =
            makeFileColumnReader(metadata, std::move(type), fileColumnIndex);
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

void NativeLanceRootColumnReader::initializeReadPlan() {
  std::unordered_map<uint32_t, ReadColumn> rowAligned;
  std::unordered_map<uint32_t, ReadColumn> offsetDependent;
  for (const auto& child : children_) {
    if (!child->readFromFile()) {
      continue;
    }
    const auto column = child->fileColumnIndex();
    auto& stage = child->readStage() == NativeLanceReadStage::kRowAligned
        ? rowAligned
        : offsetDependent;
    stage.try_emplace(
        column,
        ReadColumn{
            column,
            child.get(),
            estimateNativeLanceTypeBytesPerRow(child->type()),
            std::nullopt});
  }
  const auto materialize = [](const auto& source, auto& destination) {
    destination.reserve(source.size());
    for (const auto& [column, entry] : source) {
      destination.push_back(entry);
    }
    std::sort(
        destination.begin(),
        destination.end(),
        [](const auto& lhs, const auto& rhs) {
          return lhs.fileColumnIndex < rhs.fileColumnIndex;
        });
  };
  materialize(rowAligned, rowAlignedReadPlan_);
  materialize(offsetDependent, offsetDependentReadPlan_);
  fileColumnIndices_.reserve(
      rowAlignedReadPlan_.size() + offsetDependentReadPlan_.size());
  for (const auto& entry : rowAlignedReadPlan_) {
    fileColumnIndices_.push_back(entry.fileColumnIndex);
  }
  for (const auto& entry : offsetDependentReadPlan_) {
    fileColumnIndices_.push_back(entry.fileColumnIndex);
  }
  std::sort(fileColumnIndices_.begin(), fileColumnIndices_.end());
}

size_t NativeLanceRootColumnReader::effectiveParallelism(
    const NativeLancePageSource& source,
    const NativeLanceColumnRequest& request,
    const std::vector<ReadColumn>& plan) const {
  if (decodingExecutor_ == nullptr || decodingParallelismFactor_ <= 1 ||
      plan.size() <= 1 || request.outputSize() == 0) {
    return 1;
  }

  constexpr uint64_t kMinCellsPerWorker = 512;
  constexpr uint64_t kMinBytesPerWorker = 32 << 10;
  unsigned __int128 cells = 0;
  unsigned __int128 bytes = 0;
  size_t compressedColumns = 0;
  const auto structural = source.metadata().usesStructuralEncoding();
  for (const auto& column : plan) {
    BOLT_CHECK(column.hasCompressedData.has_value());
    const auto compressed = *column.hasCompressedData;
    compressedColumns += compressed;
    const auto rows = !structural && compressed
        ? request.rowCount
        : static_cast<uint64_t>(request.outputSize());
    cells += rows;
    bytes += static_cast<unsigned __int128>(rows) * column.estimatedBytesPerRow;
  }
  const auto workersForCells =
      (cells + kMinCellsPerWorker - 1) / kMinCellsPerWorker;
  const auto workersForBytes =
      (bytes + kMinBytesPerWorker - 1) / kMinBytesPerWorker;
  auto requestedWorkers = std::max(workersForCells, workersForBytes);
  if (compressedColumns >= 2) {
    requestedWorkers = std::max<unsigned __int128>(requestedWorkers, 2);
  }
  const auto maxWorkers = std::min(decodingParallelismFactor_, plan.size());
  return requestedWorkers >= maxWorkers
      ? maxWorkers
      : std::max<size_t>(1, static_cast<size_t>(requestedWorkers));
}

VectorPtr NativeLanceRootColumnReader::read(
    NativeLancePageSource& source,
    const NativeLanceColumnRequest& request,
    memory::MemoryPool& pool,
    bool primaryRangesPlanned,
    const std::unordered_map<uint32_t, VectorPtr>* predecodedColumns) const {
  request.validate();
  const auto cacheCompression = [&source](const auto& plan) {
    for (const auto& column : plan) {
      if (!column.hasCompressedData.has_value()) {
        column.hasCompressedData =
            source.hasCompressedColumn(column.fileColumnIndex);
      }
    }
  };
  if (request.outputSize() > 0) {
    cacheCompression(rowAlignedReadPlan_);
    cacheCompression(offsetDependentReadPlan_);
  }
  auto rowAlignedPlan = rowAlignedReadPlan_;
  auto offsetDependentPlan = offsetDependentReadPlan_;
  std::unordered_map<uint32_t, VectorPtr> decodedColumns;
  if (predecodedColumns != nullptr) {
    decodedColumns = *predecodedColumns;
    const auto removePredecoded = [&](std::vector<ReadColumn>& plan) {
      plan.erase(
          std::remove_if(
              plan.begin(),
              plan.end(),
              [&](const auto& entry) {
                return decodedColumns.contains(entry.fileColumnIndex);
              }),
          plan.end());
    };
    removePredecoded(rowAlignedPlan);
    removePredecoded(offsetDependentPlan);
  }
  decodedColumns.reserve(
      decodedColumns.size() + rowAlignedPlan.size() +
      offsetDependentPlan.size());
  const auto columns = [](const std::vector<ReadColumn>& plan) {
    std::vector<uint32_t> result;
    result.reserve(plan.size());
    for (const auto& entry : plan) {
      result.push_back(entry.fileColumnIndex);
    }
    return result;
  };
  const auto rowAlignedColumns = columns(rowAlignedPlan);
  std::vector<VectorPtr> rowAlignedResults(rowAlignedPlan.size());

  if (!primaryRangesPlanned && !rowAlignedColumns.empty()) {
    source.planColumns(rowAlignedColumns, request);
  }
  const auto rowAlignedParallelism =
      effectiveParallelism(source, request, rowAlignedPlan);
  dwio::common::ParallelFor(
      pool.threadSafe() && rowAlignedParallelism > 1 ? decodingExecutor_
                                                     : nullptr,
      0,
      rowAlignedPlan.size(),
      rowAlignedParallelism)
      .execute([&](size_t index) {
        rowAlignedResults[index] =
            rowAlignedPlan[index].reader->read(source, request, pool, true);
      });
  for (size_t index = 0; index < rowAlignedPlan.size(); ++index) {
    decodedColumns.emplace(
        rowAlignedPlan[index].fileColumnIndex,
        std::move(rowAlignedResults[index]));
  }

  const auto offsetDependentColumns = columns(offsetDependentPlan);
  if (!primaryRangesPlanned && !offsetDependentColumns.empty()) {
    source.planColumns(offsetDependentColumns, request);
  }
  std::vector<VectorPtr> offsetDependentResults(offsetDependentPlan.size());
  const auto offsetDependentParallelism =
      effectiveParallelism(source, request, offsetDependentPlan);
  dwio::common::ParallelFor(
      pool.threadSafe() && offsetDependentParallelism > 1 ? decodingExecutor_
                                                          : nullptr,
      0,
      offsetDependentPlan.size(),
      offsetDependentParallelism)
      .execute([&](size_t index) {
        offsetDependentResults[index] = offsetDependentPlan[index].reader->read(
            source, request, pool, true);
      });
  for (size_t index = 0; index < offsetDependentPlan.size(); ++index) {
    decodedColumns.emplace(
        offsetDependentPlan[index].fileColumnIndex,
        std::move(offsetDependentResults[index]));
  }

  std::vector<VectorPtr> children;
  children.reserve(children_.size());
  for (size_t channel = 0; channel < children_.size(); ++channel) {
    const auto& child = children_[channel];
    auto result = child->readFromFile()
        ? decodedColumns.at(child->fileColumnIndex())
        : child->read(source, request, pool, true);
    BOLT_CHECK_NOT_NULL(result);
    BOLT_CHECK_EQ(result->size(), request.outputSize());
    BOLT_CHECK(
        result->type()->equivalent(*outputType_->childAt(channel)),
        "Native Lance output child type does not match the scan plan");
    children.push_back(std::move(result));
  }
  return std::make_shared<RowVector>(
      &pool, outputType_, nullptr, request.outputSize(), std::move(children));
}

void NativeLanceRootColumnReader::planRead(
    NativeLancePageSource& source,
    const NativeLanceColumnRequest& request) const {
  request.validate();
  source.planColumns(fileColumnIndices_, request);
}

std::vector<uint32_t> NativeLanceRootColumnReader::fileColumnIndices() const {
  return fileColumnIndices_;
}

} // namespace bytedance::bolt::lance::reader
