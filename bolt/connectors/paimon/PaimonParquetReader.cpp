/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/connectors/paimon/PaimonParquetReader.h"
#include <folly/io/IOBuf.h>
#include <paimon/data/timestamp.h>
#include <paimon/defs.h>
#include <paimon/format/reader_builder.h>
#include <paimon/fs/file_system.h>
#include <paimon/predicate/compound_predicate.h>
#include <paimon/predicate/function.h>
#include <paimon/predicate/leaf_predicate.h>
#include <paimon/predicate/predicate.h>
#include <paimon/reader/file_batch_reader.h>
#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <vector>
#include "bolt/common/file/File.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/filter/FilterBase.h"
#include "bolt/type/filter/FilterUtil.h"
#include "bolt/type/filter/FloatingPointRange.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::connector::paimon {

namespace {

using bytedance::bolt::common::Filter;
using bytedance::bolt::common::FilterPtr;

struct PredicateConversionResult {
  std::vector<std::pair<std::string, std::unique_ptr<Filter>>> filters;
  bool fullyConvertible{true};
};

std::optional<std::string> resolveFieldName(
    const ::paimon::LeafPredicate& leaf,
    const RowTypePtr& rowType) {
  if (!leaf.FieldName().empty()) {
    return leaf.FieldName();
  }
  const auto index = leaf.FieldIndex();
  if (index >= 0 && rowType && index < rowType->size()) {
    return rowType->nameOf(index);
  }
  return std::nullopt;
}

bool literalIsNull(const ::paimon::Literal& literal) {
  return literal.IsNull();
}

int64_t literalAsInt64(
    const ::paimon::Literal& literal,
    ::paimon::FieldType type) {
  switch (type) {
    case ::paimon::FieldType::TINYINT:
      return static_cast<int64_t>(literal.GetValue<int8_t>());
    case ::paimon::FieldType::SMALLINT:
      return static_cast<int64_t>(literal.GetValue<int16_t>());
    case ::paimon::FieldType::INT:
      return static_cast<int64_t>(literal.GetValue<int32_t>());
    case ::paimon::FieldType::BIGINT:
      return static_cast<int64_t>(literal.GetValue<int64_t>());
    default:
      return 0;
  }
}

std::string literalAsString(const ::paimon::Literal& literal) {
  return literal.GetValue<std::string>();
}

bytedance::bolt::Timestamp literalAsBoltTimestamp(
    const ::paimon::Literal& literal) {
  auto ts = literal.GetValue<::paimon::Timestamp>();
  const int64_t millis = ts.GetMillisecond();
  const int32_t nanosOfMillisecond = ts.GetNanoOfMillisecond();
  auto base = bytedance::bolt::Timestamp::fromMillis(millis);
  uint64_t nanos = base.getNanos() + static_cast<uint64_t>(nanosOfMillisecond);
  auto seconds = base.getSeconds();
  if (nanos >= 1'000'000'000) {
    nanos -= 1'000'000'000;
    ++seconds;
  }
  return bytedance::bolt::Timestamp(seconds, nanos);
}

std::optional<bytedance::bolt::Timestamp> shiftTimestampNanos(
    const bytedance::bolt::Timestamp& value,
    int64_t delta) {
  const auto min = bytedance::bolt::Timestamp::min();
  const auto max = bytedance::bolt::Timestamp::max();
  const auto nanos = value.toNanos();
  const auto minNanos = min.toNanos();
  const auto maxNanos = max.toNanos();
  if (delta > 0 && nanos > maxNanos - delta) {
    return std::nullopt;
  }
  if (delta < 0 && nanos < minNanos - delta) {
    return std::nullopt;
  }
  return bytedance::bolt::Timestamp::fromNanos(nanos + delta);
}

std::unique_ptr<Filter> buildFilterForLeaf(
    const ::paimon::LeafPredicate& leaf) {
  const auto& function = leaf.GetFunction();
  const auto functionType = function.GetType();
  const auto fieldType = leaf.GetFieldType();
  const auto& literals = leaf.Literals();

  auto hasNullLiteral = [&]() {
    return std::any_of(literals.begin(), literals.end(), literalIsNull);
  };

  switch (functionType) {
    case ::paimon::Function::Type::IS_NULL:
      return bytedance::bolt::common::nullOrFalse(true);
    case ::paimon::Function::Type::IS_NOT_NULL:
      return bytedance::bolt::common::notNullOrTrue(false);
    case ::paimon::Function::Type::EQUAL: {
      if (literals.empty()) {
        return nullptr;
      }
      if (literalIsNull(literals.front())) {
        return bytedance::bolt::common::nullOrFalse(true);
      }
      switch (fieldType) {
        case ::paimon::FieldType::TINYINT:
        case ::paimon::FieldType::SMALLINT:
        case ::paimon::FieldType::INT:
        case ::paimon::FieldType::BIGINT: {
          auto value = literalAsInt64(literals.front(), fieldType);
          return bytedance::bolt::common::createBigintRange(
              value, value, false, true);
        }
        case ::paimon::FieldType::FLOAT: {
          auto value = literals.front().GetValue<float>();
          return std::make_unique<
              bytedance::bolt::common::FloatingPointRange<float>>(
              value, false, false, value, false, false, false);
        }
        case ::paimon::FieldType::DOUBLE: {
          auto value = literals.front().GetValue<double>();
          return std::make_unique<
              bytedance::bolt::common::FloatingPointRange<double>>(
              value, false, false, value, false, false, false);
        }
        case ::paimon::FieldType::STRING:
        case ::paimon::FieldType::BINARY: {
          auto value = literalAsString(literals.front());
          return bytedance::bolt::common::createBytesRange(
              value, true, value, true, false);
        }
        case ::paimon::FieldType::TIMESTAMP: {
          auto value = literalAsBoltTimestamp(literals.front());
          return std::make_unique<bytedance::bolt::common::TimestampRange>(
              value, value, false);
        }
        default:
          return nullptr;
      }
    }
    case ::paimon::Function::Type::NOT_EQUAL: {
      if (literals.empty()) {
        return nullptr;
      }
      if (literalIsNull(literals.front())) {
        return bytedance::bolt::common::notNullOrTrue(false);
      }
      switch (fieldType) {
        case ::paimon::FieldType::TINYINT:
        case ::paimon::FieldType::SMALLINT:
        case ::paimon::FieldType::INT:
        case ::paimon::FieldType::BIGINT: {
          auto value = literalAsInt64(literals.front(), fieldType);
          return std::make_unique<bytedance::bolt::common::NegatedBigintRange>(
              value, value, false);
        }
        case ::paimon::FieldType::STRING:
        case ::paimon::FieldType::BINARY: {
          auto value = literalAsString(literals.front());
          return std::make_unique<bytedance::bolt::common::NegatedBytesValues>(
              std::vector<std::string>{value}, false);
        }
        case ::paimon::FieldType::TIMESTAMP: {
          auto value = literalAsBoltTimestamp(literals.front());
          return std::make_unique<
              bytedance::bolt::common::NegatedTimestampRange>(
              value, value, false);
        }
        default:
          return nullptr;
      }
    }
    case ::paimon::Function::Type::GREATER_THAN:
    case ::paimon::Function::Type::GREATER_OR_EQUAL:
    case ::paimon::Function::Type::LESS_THAN:
    case ::paimon::Function::Type::LESS_OR_EQUAL: {
      if (literals.empty()) {
        return nullptr;
      }
      if (literalIsNull(literals.front())) {
        return nullptr;
      }
      const bool isGreater =
          functionType == ::paimon::Function::Type::GREATER_THAN ||
          functionType == ::paimon::Function::Type::GREATER_OR_EQUAL;
      const bool isExclusive =
          functionType == ::paimon::Function::Type::GREATER_THAN ||
          functionType == ::paimon::Function::Type::LESS_THAN;
      switch (fieldType) {
        case ::paimon::FieldType::TINYINT:
        case ::paimon::FieldType::SMALLINT:
        case ::paimon::FieldType::INT:
        case ::paimon::FieldType::BIGINT: {
          auto value = literalAsInt64(literals.front(), fieldType);
          auto lower = std::numeric_limits<int64_t>::min();
          auto upper = std::numeric_limits<int64_t>::max();
          if (isGreater) {
            if (isExclusive) {
              if (value == std::numeric_limits<int64_t>::max()) {
                return bytedance::bolt::common::nullOrFalse(false);
              }
              lower = value + 1;
            } else {
              lower = value;
            }
          } else {
            if (isExclusive) {
              if (value == std::numeric_limits<int64_t>::min()) {
                return bytedance::bolt::common::nullOrFalse(false);
              }
              upper = value - 1;
            } else {
              upper = value;
            }
          }
          return bytedance::bolt::common::createBigintRange(
              lower, upper, false, true);
        }
        case ::paimon::FieldType::FLOAT: {
          auto value = literals.front().GetValue<float>();
          if (isGreater) {
            return std::make_unique<
                bytedance::bolt::common::FloatingPointRange<float>>(
                value, false, isExclusive, 0.0F, true, false, false);
          }
          return std::make_unique<
              bytedance::bolt::common::FloatingPointRange<float>>(
              0.0F, true, false, value, false, isExclusive, false);
        }
        case ::paimon::FieldType::DOUBLE: {
          auto value = literals.front().GetValue<double>();
          if (isGreater) {
            return std::make_unique<
                bytedance::bolt::common::FloatingPointRange<double>>(
                value, false, isExclusive, 0.0, true, false, false);
          }
          return std::make_unique<
              bytedance::bolt::common::FloatingPointRange<double>>(
              0.0, true, false, value, false, isExclusive, false);
        }
        case ::paimon::FieldType::STRING:
        case ::paimon::FieldType::BINARY: {
          auto value = literalAsString(literals.front());
          if (isGreater) {
            return bytedance::bolt::common::createBytesRange(
                value, !isExclusive, std::nullopt, false, false);
          }
          return bytedance::bolt::common::createBytesRange(
              std::nullopt, false, value, !isExclusive, false);
        }
        case ::paimon::FieldType::TIMESTAMP: {
          auto value = literalAsBoltTimestamp(literals.front());
          if (isGreater) {
            if (!isExclusive) {
              return std::make_unique<bytedance::bolt::common::TimestampRange>(
                  value, bytedance::bolt::Timestamp::max(), false);
            }
            auto lower = shiftTimestampNanos(value, 1);
            if (!lower.has_value()) {
              return bytedance::bolt::common::nullOrFalse(false);
            }
            return std::make_unique<bytedance::bolt::common::TimestampRange>(
                lower.value(), bytedance::bolt::Timestamp::max(), false);
          }
          if (!isExclusive) {
            return std::make_unique<bytedance::bolt::common::TimestampRange>(
                bytedance::bolt::Timestamp::min(), value, false);
          }
          auto upper = shiftTimestampNanos(value, -1);
          if (!upper.has_value()) {
            return bytedance::bolt::common::nullOrFalse(false);
          }
          return std::make_unique<bytedance::bolt::common::TimestampRange>(
              bytedance::bolt::Timestamp::min(), upper.value(), false);
        }
        default:
          return nullptr;
      }
    }
    case ::paimon::Function::Type::IN: {
      if (literals.empty()) {
        return nullptr;
      }
      const bool nullAllowed = hasNullLiteral();
      switch (fieldType) {
        case ::paimon::FieldType::TINYINT:
        case ::paimon::FieldType::SMALLINT:
        case ::paimon::FieldType::INT:
        case ::paimon::FieldType::BIGINT: {
          std::vector<int64_t> values;
          values.reserve(literals.size());
          for (const auto& literal : literals) {
            if (!literalIsNull(literal)) {
              values.push_back(literalAsInt64(literal, fieldType));
            }
          }
          return bytedance::bolt::common::createBigintValues(
              values, nullAllowed);
        }
        case ::paimon::FieldType::STRING:
        case ::paimon::FieldType::BINARY: {
          std::vector<std::string> values;
          values.reserve(literals.size());
          for (const auto& literal : literals) {
            if (!literalIsNull(literal)) {
              values.emplace_back(literalAsString(literal));
            }
          }
          return bytedance::bolt::common::createBytesValues(
              values, nullAllowed);
        }
        default:
          return nullptr;
      }
    }
    case ::paimon::Function::Type::NOT_IN: {
      if (literals.empty()) {
        return nullptr;
      }
      const bool nullAllowed = hasNullLiteral();
      switch (fieldType) {
        case ::paimon::FieldType::TINYINT:
        case ::paimon::FieldType::SMALLINT:
        case ::paimon::FieldType::INT:
        case ::paimon::FieldType::BIGINT: {
          std::vector<int64_t> values;
          values.reserve(literals.size());
          for (const auto& literal : literals) {
            if (!literalIsNull(literal)) {
              values.push_back(literalAsInt64(literal, fieldType));
            }
          }
          return bytedance::bolt::common::createNegatedBigintValues(
              values, nullAllowed);
        }
        case ::paimon::FieldType::STRING:
        case ::paimon::FieldType::BINARY: {
          std::vector<std::string> values;
          values.reserve(literals.size());
          for (const auto& literal : literals) {
            if (!literalIsNull(literal)) {
              values.emplace_back(literalAsString(literal));
            }
          }
          return std::make_unique<bytedance::bolt::common::NegatedBytesValues>(
              values, nullAllowed);
        }
        default:
          return nullptr;
      }
    }
    default:
      return nullptr;
  }
}

bool collectSameColumnOrFilters(
    const std::vector<std::shared_ptr<::paimon::Predicate>>& children,
    const RowTypePtr& rowType,
    PredicateConversionResult& result) {
  std::optional<std::string> columnName;
  std::optional<::paimon::FieldType> fieldType;
  std::vector<int64_t> intValues;
  std::vector<std::string> stringValues;
  bool allEqual = true;

  for (const auto& child : children) {
    auto leaf = std::dynamic_pointer_cast<::paimon::LeafPredicate>(child);
    if (!leaf) {
      return false;
    }
    if (leaf->GetFunction().GetType() != ::paimon::Function::Type::EQUAL) {
      allEqual = false;
      break;
    }
    auto name = resolveFieldName(*leaf, rowType);
    if (!name.has_value()) {
      return false;
    }
    if (!columnName.has_value()) {
      columnName = name;
      fieldType = leaf->GetFieldType();
    } else if (columnName.value() != name.value()) {
      return false;
    }

    if (leaf->Literals().empty() || literalIsNull(leaf->Literals().front())) {
      return false;
    }

    if (fieldType == ::paimon::FieldType::TINYINT ||
        fieldType == ::paimon::FieldType::SMALLINT ||
        fieldType == ::paimon::FieldType::INT ||
        fieldType == ::paimon::FieldType::BIGINT) {
      intValues.push_back(
          literalAsInt64(leaf->Literals().front(), fieldType.value()));
    } else if (
        fieldType == ::paimon::FieldType::STRING ||
        fieldType == ::paimon::FieldType::BINARY) {
      stringValues.push_back(literalAsString(leaf->Literals().front()));
    } else {
      return false;
    }
  }

  if (!allEqual || !columnName.has_value() || !fieldType.has_value()) {
    return false;
  }

  std::unique_ptr<Filter> filter;
  if (fieldType == ::paimon::FieldType::TINYINT ||
      fieldType == ::paimon::FieldType::SMALLINT ||
      fieldType == ::paimon::FieldType::INT ||
      fieldType == ::paimon::FieldType::BIGINT) {
    filter = bytedance::bolt::common::createBigintValues(intValues, false);
  } else {
    filter = bytedance::bolt::common::createBytesValues(stringValues, false);
  }

  result.filters.emplace_back(columnName.value(), std::move(filter));
  return true;
}

PredicateConversionResult convertPredicateToFilters(
    const std::shared_ptr<::paimon::Predicate>& predicate,
    const RowTypePtr& rowType) {
  PredicateConversionResult result;
  if (!predicate) {
    return result;
  }

  // Iterative traversal to satisfy clang-tidy misc-no-recursion.
  std::vector<std::shared_ptr<::paimon::Predicate>> stack;
  stack.push_back(predicate);

  while (!stack.empty()) {
    auto current = std::move(stack.back());
    stack.pop_back();
    if (!current) {
      continue;
    }

    if (auto leaf =
            std::dynamic_pointer_cast<::paimon::LeafPredicate>(current)) {
      auto name = resolveFieldName(*leaf, rowType);
      if (!name.has_value()) {
        result.fullyConvertible = false;
        continue;
      }
      auto filter = buildFilterForLeaf(*leaf);
      if (!filter) {
        result.fullyConvertible = false;
        continue;
      }
      result.filters.emplace_back(name.value(), std::move(filter));
      continue;
    }

    auto compound =
        std::dynamic_pointer_cast<::paimon::CompoundPredicate>(current);
    if (!compound) {
      result.fullyConvertible = false;
      continue;
    }

    const auto functionType = compound->GetFunction().GetType();
    const auto& children = compound->Children();
    if (functionType == ::paimon::Function::Type::AND) {
      // Flatten AND by pushing children onto the stack.
      for (const auto& child : children) {
        stack.push_back(child);
      }
      continue;
    }

    if (functionType == ::paimon::Function::Type::OR) {
      if (!collectSameColumnOrFilters(children, rowType, result)) {
        result.fullyConvertible = false;
      }
      continue;
    }

    result.fullyConvertible = false;
  }

  return result;
}

void applyFiltersToScanSpec(
    bolt::common::ScanSpec& scanSpec,
    PredicateConversionResult& result) {
  for (auto& entry : result.filters) {
    auto* child = scanSpec.getOrCreateChild(entry.first);
    if (!child) {
      continue;
    }
    if (entry.second) {
      child->addFilter(*entry.second);
    }
  }
}

class PaimonParquetFileBatchReader : public ::paimon::FileBatchReader {
 private:
  static std::shared_ptr<bolt::common::ScanSpec> buildScanSpecFromRowType(
      const RowTypePtr& rowType) {
    auto scanSpec = std::make_shared<bolt::common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*rowType);
    return scanSpec;
  }

  void initializeRowReaderWithFullSchema() {
    // LOG(INFO) << "Initializing rowReader_ with full file schema: " <<
    // reader_->rowType()->toString();
    dwio::common::RowReaderOptions opts;
    opts.setScanSpec(buildScanSpecFromRowType(reader_->rowType()));
    rowReader_ = reader_->createRowReader(opts);
  }

 public:
  PaimonParquetFileBatchReader(
      std::unique_ptr<parquet::ParquetReader> reader,
      int32_t batch_size,
      memory::MemoryPool* const pool)
      : reader_(std::move(reader)),
        batch_size_(batch_size),
        pool_(std::move(pool)),
        readType_(reader_->rowType()) {
    // LOG(INFO) << "PaimonParquetFileBatchReader created, reader_->rowType() =
    // " << reader_->rowType()->toString();
  }

  ::paimon::Result<std::unique_ptr<::ArrowSchema>> GetFileSchema()
      const override {
    auto schema = std::make_unique<::ArrowSchema>();

    const auto& fileRowType = reader_->rowType();
    // LOG(INFO) << "GetFileSchema: file schema = " << fileRowType->toString();

    auto dummyVector = BaseVector::create(fileRowType, 0, pool_);

    ArrowOptions opts;
    exportToArrow(dummyVector, *schema, opts);

    LOG(INFO) << "GetFileSchema exported ArrowSchema has " << schema->n_children
              << " children";
    for (int i = 0; i < schema->n_children; ++i) {
      LOG(INFO) << "GetFileSchema child[" << i << "]: name="
                << (schema->children[i]->name ? schema->children[i]->name : "")
                << ", format="
                << (schema->children[i]->format ? schema->children[i]->format
                                                : "");
    }
    return std::move(schema);
  }

  ::paimon::Status SetReadSchema(
      ::ArrowSchema* read_schema,
      const std::shared_ptr<::paimon::Predicate>& predicate,
      const std::optional<::paimon::RoaringBitmap32>& /*selection_bitmap*/)
      override {
    try {
      auto type = importFromArrow(*read_schema);
      auto rowType = std::dynamic_pointer_cast<const RowType>(type);
      if (!rowType) {
        return ::paimon::Status::Invalid(
            "Read schema must be a struct/row type");
      }

      std::vector<std::string> dataColumnNames;
      int startIndex = 0;
      for (int i = startIndex; i < rowType->size(); ++i) {
        dataColumnNames.push_back(rowType->nameOf(i));
      }

      dwio::common::RowReaderOptions opts;
      auto fileRowType = reader_->rowType();

      auto scanSpec = buildScanSpecFromRowType(rowType);
      if (predicate) {
        auto conversion = convertPredicateToFilters(predicate, rowType);
        applyFiltersToScanSpec(*scanSpec, conversion);
      }
      opts.setScanSpec(scanSpec);
      auto selector = std::make_shared<dwio::common::ColumnSelector>(
          fileRowType, dataColumnNames);
      opts.select(selector);
      rowReader_ = reader_->createRowReader(opts);
      readType_ = rowType;

      return ::paimon::Status::OK();
    } catch (const std::exception& e) {
      LOG(ERROR) << "SetReadSchema: exception " << e.what();
      return ::paimon::Status::Invalid(
          std::string("Failed to set read schema: ") + e.what());
    }
  }

  ::paimon::Result<ReadBatch> NextBatch() override {
    try {
      if (!rowReader_) {
        LOG(INFO)
            << "NextBatch: rowReader_ not initialized, initializing with full schema";
        initializeRowReaderWithFullSchema();
      }
      VectorPtr result = BaseVector::create(readType_, batch_size_, pool_);
      bool hasData = rowReader_->next(batch_size_, result) != 0;

      if (!hasData) {
        LOG(INFO) << "NextBatch: End of file reached";
        return ::paimon::BatchReader::MakeEofBatch();
      }

      // LOG(INFO) << "NextBatch: result has type " <<
      // result->type()->toString(); LOG(INFO) << "NextBatch: number of rows in
      // batch = " << result->size();

      auto arrowArray = std::make_unique<::ArrowArray>();
      auto arrowSchema = std::make_unique<::ArrowSchema>();

      ArrowOptions opts;
      exportToArrow(result, *arrowArray, pool_, opts);
      exportToArrow(result, *arrowSchema, opts);

      LOG(INFO) << "NextBatch: exported ArrowSchema has "
                << arrowSchema->n_children << " children";
      // for (int i = 0; i < arrowSchema->n_children; ++i) {
      //   LOG(INFO) << "NextBatch exported child[" << i << "]: name=" <<
      //   (arrowSchema->children[i]->name ? arrowSchema->children[i]->name :
      //   "")
      //             << ", format=" << (arrowSchema->children[i]->format ?
      //             arrowSchema->children[i]->format : "");
      // }
      // LOG(INFO) << "NextBatch: exported ArrowArray has length " <<
      // arrowArray->length;

      return std::make_pair(std::move(arrowArray), std::move(arrowSchema));
    } catch (const std::exception& e) {
      LOG(ERROR) << "NextBatch: exception " << e.what();
      return ::paimon::Status::IOError(
          std::string("Failed to read batch: ") + e.what());
    }
  }

  std::shared_ptr<::paimon::Metrics> GetReaderMetrics() const override {
    return nullptr;
  }

  void Close() override {
    rowReader_.reset();
    reader_.reset();
    if (pool_) {
      pool_->release();
    }
  }

  uint64_t GetPreviousBatchFirstRowNumber() const override {
    return 0;
  }
  ::paimon::Result<uint64_t> GetNumberOfRows() const override {
    auto numRows = reader_->numberOfRows();
    if (numRows) {
      return *numRows;
    }
    return ::paimon::Status::Invalid("Number of rows not available");
  }
  bool SupportPreciseBitmapSelection() const override {
    return false;
  }

 private:
  std::unique_ptr<parquet::ParquetReader> reader_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;
  int32_t batch_size_;
  memory::MemoryPool* const pool_;
  RowTypePtr readType_;
};

class PaimonParquetReaderBuilder : public ::paimon::ReaderBuilder {
 public:
  explicit PaimonParquetReaderBuilder(int32_t batch_size)
      : batch_size_(batch_size) {}

  ::paimon::ReaderBuilder* WithMemoryPool(
      const std::shared_ptr<::paimon::MemoryPool>& pool) override {
    auto boltPool = std::dynamic_pointer_cast<BoltPaimonMemoryPool>(pool);
    if (boltPool != nullptr) {
      paimonPool_ = boltPool;
    }
    return this;
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileBatchReader>> Build(
      const std::shared_ptr<::paimon::InputStream>& path) const override {
    BOLT_CHECK_NOT_NULL(
        paimonPool_,
        "PaimonParquetReaderBuilder requires WithMemoryPool to be called before Build");
    try {
      auto rf = std::make_shared<PaimonReadFile>(path);
      auto input = std::make_unique<dwio::common::BufferedInput>(
          std::make_shared<dwio::common::ReadFileInputStream>(rf),
          *paimonPool_->getBoltPool());

      dwio::common::ReaderOptions readerOptions(paimonPool_->getBoltPool());
      auto reader = std::make_unique<parquet::ParquetReader>(
          std::move(input), readerOptions);

      return std::make_unique<PaimonParquetFileBatchReader>(
          std::move(reader), batch_size_, paimonPool_->getBoltPool());
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("Failed to build reader from InputStream: ") + e.what());
    }
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileBatchReader>> Build(
      const std::string& path) const override {
    BOLT_CHECK_NOT_NULL(
        paimonPool_,
        "PaimonParquetReaderBuilder requires WithMemoryPool to be called before Build");
    try {
      auto file = std::make_shared<LocalReadFile>(path);
      memory::MemoryPool* boltPool = paimonPool_->getBoltPool();

      auto input =
          std::make_unique<dwio::common::BufferedInput>(file, *boltPool);

      dwio::common::ReaderOptions readerOptions(boltPool);
      auto reader = std::make_unique<parquet::ParquetReader>(
          std::move(input), readerOptions);

      return std::make_unique<PaimonParquetFileBatchReader>(
          std::move(reader), batch_size_, paimonPool_->getBoltPool());
    } catch (const std::exception& e) {
      return ::paimon::Status::IOError(
          std::string("Failed to open file: ") + e.what());
    }
  }

 private:
  int32_t batch_size_;
  std::shared_ptr<BoltPaimonMemoryPool> paimonPool_;
};

} // namespace

const std::string& PaimonParquetReader::Identifier() const {
  static const std::string kIdentifier = "parquet";
  return kIdentifier;
}

::paimon::Result<std::unique_ptr<::paimon::ReaderBuilder>>
PaimonParquetReader::CreateReaderBuilder(int32_t batch_size) const {
  return std::make_unique<PaimonParquetReaderBuilder>(batch_size);
}

::paimon::Result<std::unique_ptr<::paimon::WriterBuilder>>
PaimonParquetReader::CreateWriterBuilder(
    ::ArrowSchema* /* schema */,
    int32_t /* batch_size */) const {
  return ::paimon::Status::NotImplemented("Writer not supported yet");
}

::paimon::Result<std::unique_ptr<::paimon::FormatStatsExtractor>>
PaimonParquetReader::CreateStatsExtractor(::ArrowSchema* /*schema*/) const {
  return ::paimon::Status::NotImplemented("Stats extractor not supported yet");
}

void EnsurePaimonParquetFormatRegistered() {
  ::paimon::ensureParquetFormatFactoryRegistered();
}

} // namespace bytedance::bolt::connector::paimon

namespace paimon {

Result<std::unique_ptr<::paimon::FileFormat>> ParquetFileFormatFactory::Create(
    const std::map<std::string, std::string>& options) const {
  return std::make_unique<
      bytedance::bolt::connector::paimon::PaimonParquetReader>(options);
}

// Explicit registration function (called from
// EnsurePaimonParquetFormatRegistered in PaimonDataSource). Using an explicit
// call rather than REGISTER_PAIMON_FACTORY macro because the linker may strip
// static
// __attribute__((constructor)) functions from object files inside static
// archives when no symbol explicitly references them.
void ensureParquetFormatFactoryRegistered() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    LOG(INFO)
        << "[PAIMON] Registering bolt ParquetFileFormatFactory with identifier='"
        << ParquetFileFormatFactory::kIDENTIFIER << "'";
    auto* factory = new ParquetFileFormatFactory;
    ::paimon::FactoryCreator::GetInstance()->Register(
        factory->Identifier(), factory);
    LOG(INFO) << "[PAIMON] ParquetFileFormatFactory registration complete";
  });
}

} // namespace paimon
