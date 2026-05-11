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

#include "bolt/shuffle/sparksql/ShuffleReaderNode.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/vector/LazyComplexCodec.h"
using bytedance::bolt::RowVectorPtr;
using bytedance::bolt::shuffle::sparksql::SparkShuffleReader;
using bytedance::bolt::shuffle::sparksql::SparkShuffleReaderNode;

SparkShuffleReader::SparkShuffleReader(
    int32_t operatorId,
    bytedance::bolt::exec::DriverCtx* driverCtx,
    std::shared_ptr<const SparkShuffleReaderNode> shuffleReaderNode)
    : bytedance::bolt::exec::SourceOperator(
          driverCtx,
          shuffleReaderNode->outputType(),
          operatorId,
          shuffleReaderNode->id(),
          std::string(shuffleReaderNode->name())),
      shuffleReaderOptions_(shuffleReaderNode->getShuffleReaderOptions()),
      readerStreamIterator_(shuffleReaderNode->getReaderStreams()),
      arrowPool_(std::make_shared<BoltArrowMemoryPool>(pool())),
      codec_(createCodec(
          shuffleReaderOptions_.compressionType,
          CodecOptions{
              getCodecBackend(shuffleReaderOptions_.codecBackend),
              kDefaultCompressionLevel,
              shuffleReaderOptions_.checksumEnabled})),
      batchSize_(shuffleReaderOptions_.batchSize),
      shuffleBatchByteSize_(shuffleReaderOptions_.shuffleBatchByteSize),
      numPartitions_(shuffleReaderOptions_.numPartitions),
      shuffleWriterType_(static_cast<ShuffleWriterType>(
          shuffleReaderOptions_.forceShuffleWriterType)),
      partitioningShortName_(shuffleReaderOptions_.partitionShortName),
      rowBufferPool_(std::make_shared<RowBufferPool>(arrowPool_.get())),
      // When a lazy codec is active, the wire schema has complex
      // positions replaced by VARBINARY. Use that schema to drive the
      // Arrow deserialiser; wrap the resulting VARBINARY children back
      // as LazyComplexVector before returning from getOutput().
      wireOutputType_(lazyBundleWireRowType(shuffleReaderNode->outputType())),
      row2ColConverter_(std::make_shared<ShuffleRowToColumnarConverter>(
          wireOutputType_,
          pool())) {
  isValidityBuffer_.reserve(wireOutputType_->size());
  for (size_t i = 0; i < wireOutputType_->size(); ++i) {
    switch (wireOutputType_->childAt(i)->kind()) {
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
        isValidityBuffer_.push_back(false);
      } break;
      case TypeKind::ARRAY:
      case TypeKind::MAP:
      case TypeKind::ROW: {
        hasComplexType_ = true;
      } break;
      case TypeKind::BOOLEAN: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(true);
      } break;
      case TypeKind::UNKNOWN:
        break;
      default: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
      } break;
    }
  }

  // must be same as BoltShuffleWriter::decideBoltShuffleWriterType
  auto partitioning = toPartitioning(partitioningShortName_);
  isRowBased_ = supportAdaptiveShuffleWriter(partitioning) &&
      ((shuffleWriterType_ == ShuffleWriterType::Adaptive &&
        numPartitions_ >= rowBasePartitionThreshold &&
        outputType_->size() >= rowBaseColumnNumThreshold) ||
       (shuffleWriterType_ == ShuffleWriterType::RowBased));
}

void SparkShuffleReader::init() {
  schema_ = boltTypeToArrowSchema(wireOutputType_, pool());
  zstdCodec_ = std::make_shared<AdaptiveParallelZstdCodec>(
      1 /*not used*/, false, arrowPool_.get());
}

bytedance::bolt::RowVectorPtr SparkShuffleReader::getOutput() {
  std::call_once(initFlag_, &SparkShuffleReader::init, this);
  while (true) {
    if (!columnarBatchDeserializer_) {
      auto in = readerStreamIterator_->nextStream(arrowPool_.get());
      if (in) {
        columnarBatchDeserializer_ =
            std::make_unique<BoltColumnarBatchDeserializer>(
                std::move(in),
                schema_,
                codec_,
                wireOutputType_,
                batchSize_,
                shuffleBatchByteSize_,
                arrowPool_.get(),
                pool(),
                &isValidityBuffer_,
                hasComplexType_,
                deserializeTime_,
                decompressTime_,
                isRowBased_,
                zstdCodec_.get(),
                rowBufferPool_.get(),
                row2ColConverter_.get());
      } else {
        finished_ = true;
        return nullptr;
      }
    }

    auto output = columnarBatchDeserializer_->next();
    if (output) {
      // Wrap VARBINARY wire children at complex positions back as
      // LazyComplexVector of the original type. No-op when codec is
      // inactive or wire already matches outputType_.
      return fromLazyBundleWireRowVector(output, outputType_, pool());
    } else {
      columnarBatchDeserializer_ = nullptr;
    }
  }
}

void SparkShuffleReader::close() {
  auto stats = this->stats().rlock();
  readerStreamIterator_->updateMetrics(
      stats->outputPositions,
      stats->outputVectors,
      decompressTime_,
      deserializeTime_,
      stats->getOutputTiming.wallNanos);
  if (readerStreamIterator_) {
    readerStreamIterator_->close();
    readerStreamIterator_ = nullptr;
  }
  bytedance::bolt::exec::SourceOperator::close();
}
