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

#include "bolt/dwio/common/fuzzer/DwioFuzzer.h"
#include "arrow/c/bridge.h"
#include "arrow/memory_pool.h"
#include "bolt/common/base/Fs.h"
#include "bolt/common/file/File.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/dwio/common/ReaderFactory.h"
#include "bolt/dwio/common/WriterFactory.h"
#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"
namespace bytedance::bolt::dwio::fuzzer {

namespace {
void assertEqualVector(
    const VectorPtr& expected,
    const VectorPtr& actual,
    vector_size_t offset = 0) {
  BOLT_CHECK_GE(expected->size(), actual->size() + offset);
  ASSERT_EQ(expected->typeKind(), actual->typeKind());

  for (vector_size_t i = 0; i < actual->size(); i++) {
    BOLT_CHECK(
        expected->equalValueAt(actual.get(), i + offset, i),
        "schema: {}, at {}: expected {}, but got {}.",
        expected->type()->toString(),
        (i + offset),
        expected->toString(i + offset),
        actual->toString(i));
  }
}

void assertRead(
    std::shared_ptr<const RowType> outputType,
    dwio::common::RowReader& reader,
    const std::vector<RowVectorPtr>& expectedData,
    memory::MemoryPool* pool) {
  VectorPtr result = BaseVector::create(outputType, 0, pool);
  for (const auto& expected : expectedData) {
    auto expectedSize = expected->size();
    size_t readSize = 0;
    while (readSize < expectedSize) {
      auto part =
          reader.next(std::min<size_t>(expectedSize - readSize, 1024), result);
      if (part > 0) {
        assertEqualVector(expected, result, readSize);
        readSize += result->size();
      } else {
        break;
      }
    }
    BOLT_CHECK_EQ(readSize, expectedSize);
  }
  BOLT_CHECK_EQ(reader.next(1000, result), 0);
}

void setNullValueSizeToZero(VectorPtr data) {
  auto typeKind = data->type()->kind();
  switch (typeKind) {
    case TypeKind::ROW: {
      std::vector<VectorPtr> children;
      for (auto& child : data->as<RowVector>()->children()) {
        if (child->encoding() != VectorEncoding::Simple::CONSTANT) {
          children.push_back(child);
        }
      }
      if (data->mayHaveNulls()) {
        for (int i = 0; i < data->size(); ++i) {
          if (!data->isNullAt(i)) {
            continue;
          }
          for (auto& child : children) {
            child->setNull(i, true);
          }
        }
      }
      for (auto& child : children) {
        setNullValueSizeToZero(child);
      }
      break;
    }
    case TypeKind::ARRAY:
    case TypeKind::MAP: {
      auto arrayVectorBase = std::static_pointer_cast<ArrayVectorBase>(data);
      if (data->mayHaveNulls()) {
        auto rawOffsets = arrayVectorBase->mutableOffsets(data->size())
                              ->asMutable<vector_size_t>();
        auto rawSizes = arrayVectorBase->mutableSizes(data->size())
                            ->asMutable<vector_size_t>();
        for (int i = 0; i < data->size(); ++i) {
          if (arrayVectorBase->isNullAt(i)) {
            rawOffsets[i] = 0;
            rawSizes[i] = 0;
          }
        }
      }
      if (typeKind == TypeKind::ARRAY) {
        setNullValueSizeToZero(arrayVectorBase->as<ArrayVector>()->elements());
      } else {
        setNullValueSizeToZero(arrayVectorBase->as<MapVector>()->mapValues());
      }
      break;
    }
    default:
      break;
  }
}
} // namespace

DwioFuzzer::DwioFuzzer(
    size_t initialSeed,
    const Options& options,
    common::FileFormat fileFormat)
    : tempPath_(exec::test::TempDirectoryPath::create()),
      options_(options),
      fileFormat_{fileFormat},
      aggregatePool_(memory::memoryManager()->addRootPool()),
      pool_(aggregatePool_->addLeafChild("leaf")),
      vectorFuzzer_(std::make_shared<VectorFuzzer>(
          options_.vectorFuzzerOptions,
          pool_.get())) {
  seed(initialSeed);
}

void DwioFuzzer::go() {
  auto startTime = std::chrono::system_clock::now();
  size_t i = 0;
  RowTypePtr schema;
  std::string filePath;
  try {
    while (!isDone(i, startTime)) {
      LOG_EVERY_T(WARNING, 5)
          << "==============================> Started iteration " << i
          << " (seed: " << currentSeed_ << ")";
      schema = fuzzSchema(rand<int32_t>(1, options_.maxSchemaColumnNum));
      auto data = fuzzData(rand<int32_t>(1, options_.maxBatchNum), schema);
      if (options_.isAssertArrowData) {
        assertArrowData(data);
      }

      filePath = fmt::format(
          "{}/{}{}.{}",
          tempPath_->path,
          toString(fileFormat_),
          i,
          toString(fileFormat_));
      writeToFile(filePath, schema, data);

      auto reader = createLocalReader(filePath);
      int64_t numberOfRows{0};
      for (const auto& v : data) {
        numberOfRows += v->size();
      }
      BOLT_CHECK(reader->numberOfRows().has_value());
      BOLT_CHECK_EQ(reader->numberOfRows().value(), numberOfRows);
      BOLT_CHECK(
          reader->rowType()->equivalent(*schema),
          "reader rowType: {}\nschema: {}.",
          reader->rowType()->toString(),
          schema->toString());

      auto rowReader = createRowReaderWithSchema(std::move(reader), schema);
      assertRead(schema, *rowReader, data, pool_.get());
      fs::remove(filePath);
      reSeed();
      ++i;
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "DwioFuzzer error, current seed: " << currentSeed_
                 << ", schema: " << (schema ? schema->toString() : "null")
                 << ", file path: " << filePath;
    throw;
  }
}

void DwioFuzzer::reSeed() {
  seed(rng_());
}

void DwioFuzzer::seed(size_t seed) {
  currentSeed_ = seed;
  vectorFuzzer_->reSeed(currentSeed_);
  rng_.seed(currentSeed_);
}

bool DwioFuzzer::coinToss(double n) {
  return boost::random::uniform_01<double>()(rng_) < n;
}

template <typename T>
bool DwioFuzzer::isDone(size_t i, T startTime) const {
  if (options_.durationSeconds > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= options_.durationSeconds;
  } else if (options_.steps > 0) {
    return i >= options_.steps;
  }
  return false;
}

RowTypePtr DwioFuzzer::fuzzSchema(int32_t columnNum) {
  static std::vector<TypePtr> kScalarTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
      DECIMAL(18, 5),
      DECIMAL(38, 18)};
  return vectorFuzzer_->randRowType(kScalarTypes);
}

std::vector<RowVectorPtr> DwioFuzzer::fuzzData(
    int32_t batchNums,
    const RowTypePtr& schema) {
  std::vector<RowVectorPtr> data;
  data.reserve(batchNums);
  for (int i = 0; i < batchNums; ++i) {
    auto rowVector = vectorFuzzer_->fuzzRow(
        schema, rand<vector_size_t>(1, options_.maxBatchSize), false);
    setNullValueSizeToZero(rowVector);
    data.push_back(std::move(rowVector));
  }
  return data;
}

void DwioFuzzer::assertArrowData(const std::vector<RowVectorPtr>& data) {
  static ArrowOptions arrowOptions{
      .flattenDictionary = true,
      .flattenConstant = true,
      .timestampUnit = TimestampUnit::kMicro,
      .timestampTimeZone = "UTC"};
  for (const auto& vector : data) {
    ArrowSchema schema;
    ArrowArray array;
    exportToArrow(vector, schema, arrowOptions);
    exportToArrow(vector, array, pool_.get(), arrowOptions);

    auto vectorFromArrow =
        importFromArrowAsOwner(schema, array, arrowOptions, pool_.get());
    assertEqualVector(vector, vectorFromArrow);
  }
}

void DwioFuzzer::writeToFile(
    const std::string& path,
    const RowTypePtr& schema,
    const std::vector<RowVectorPtr>& data) {
  BOLT_CHECK_GT(data.size(), 0);

  dwio::common::WriterOptions options;
  options.schema = schema;
  options.memoryPool = pool_.get();

  if (fs::exists(path)) {
    fs::remove(path);
    LOG(WARNING) << fmt::format("File {} deleted successfully.", path);
  }
  auto writeFile = std::make_unique<LocalWriteFile>(path, true, true);
  auto sink =
      std::make_unique<dwio::common::WriteFileSink>(std::move(writeFile), path);
  std::unique_ptr<dwio::common::Writer> writer;
  switch (fileFormat_) {
    case common::FileFormat::PARQUET:
      writer =
          createFuzzerParuqetWriter(std::move(sink), asRowType(options.schema));
      break;
    default:
      BOLT_FAIL("unsupported file format: {}.", toString(fileFormat_));
  }
  for (const auto& vector : data) {
    writer->write(vector);
  }
  writer->close();
}

std::unique_ptr<dwio::common::Writer> DwioFuzzer::createFuzzerParuqetWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    RowTypePtr schema) {
  bolt::parquet::WriterOptions writerOptions;
  writerOptions.enableFlushBasedOnBlockSize = true;
  writerOptions.writeInt96AsTimestamp = true;
  writerOptions.parquetWriteTimestampUnit = TimestampUnit::kMicro;
  writerOptions.parquetWriteTimestampTimeZone = "UTC";

  writerOptions.parquet_block_size = rand<int64_t>(
      std::min<int64_t>(1024 * 1024, options_.maxBlockSize),
      options_.maxBlockSize);
  writerOptions.parquetVersion = coinToss(0.8)
      ? parquet::arrow::ParquetVersion::PARQUET_2_6
      : parquet::arrow::ParquetVersion::PARQUET_1_0;
  writerOptions.storeDecimalAsInteger = coinToss(0.5);
  writerOptions.writeBatchBytes = rand<int64_t>(1024 * 1024, 10 * 1024 * 1024);
  writerOptions.minBatchSize = rand<int32_t>(1, options_.maxBatchSize);
  writerOptions.enableDictionary = coinToss(0.8);
  writerOptions.dictionaryPageSizeLimit = rand<int64_t>(32, 1024 * 1024);
  writerOptions.dataPageSize = rand<int64_t>(32, 1024 * 1024);
  writerOptions.dataPageVersion = coinToss(0.5)
      ? parquet::arrow::ParquetDataPageVersion::V1
      : parquet::arrow::ParquetDataPageVersion::V2;

  static std::vector<bolt::common::CompressionKind> compressionKind{
      bolt::common::CompressionKind::CompressionKind_NONE,
      bolt::common::CompressionKind::CompressionKind_SNAPPY,
      bolt::common::CompressionKind::CompressionKind_ZSTD,
      bolt::common::CompressionKind::CompressionKind_LZ4,
      bolt::common::CompressionKind::CompressionKind_GZIP};
  writerOptions.compression =
      compressionKind[rand<int32_t>(0, compressionKind.size() - 1)];

  ArrowSchema cArrowSchema;
  exportToArrow(
      bolt::BaseVector::create(schema, 0, pool_.get()),
      cArrowSchema,
      ArrowOptions{
          .flattenDictionary = options_.arrowFlattenDictionary,
          .flattenConstant = options_.arrowFlattenConstant,
          .useLargeString = true});
  auto arrowSchema = ::arrow::ImportSchema(&cArrowSchema).ValueOrDie();
  std::vector<std::shared_ptr<::arrow::Field>> newFields;
  auto childSize = arrowSchema->num_fields();
  for (auto i = 0; i < childSize; i++) {
    newFields.push_back(arrowSchema->field(i)->WithNullable(coinToss(0.8)));
  }

  return std::make_unique<parquet::Writer>(
      std::move(sink),
      writerOptions,
      aggregatePool_,
      ::arrow::default_memory_pool(),
      schema,
      // ::arrow::schema(newFields));
      nullptr);
}

std::unique_ptr<common::Reader> DwioFuzzer::createLocalReader(
    const std::string& filePath) {
  common::ReaderOptions readerOptions{pool_.get()};
  switch (fileFormat_) {
    case common::FileFormat::PARQUET:
      return std::make_unique<parquet::ParquetReader>(
          std::make_unique<common::BufferedInput>(
              std::make_shared<LocalReadFile>(filePath),
              readerOptions.getMemoryPool()),
          readerOptions);
      break;
    default:
      BOLT_FAIL("unsupported file format: {}.", toString(fileFormat_));
  }
}

std::unique_ptr<common::RowReader> DwioFuzzer::createRowReaderWithSchema(
    const std::unique_ptr<common::Reader> reader,
    const RowTypePtr& rowType) {
  dwio::common::RowReaderOptions rowReaderOpts;
  rowReaderOpts.setTimestampPrecision(TimestampPrecision::kMicroseconds);
  rowReaderOpts.select(
      std::make_shared<bytedance::bolt::dwio::common::ColumnSelector>(
          rowType, rowType->names(), nullptr, false));
  auto scanSpec = std::make_shared<bolt::common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  rowReaderOpts.setScanSpec(scanSpec);
  return reader->createRowReader(rowReaderOpts);
};
} // namespace bytedance::bolt::dwio::fuzzer
