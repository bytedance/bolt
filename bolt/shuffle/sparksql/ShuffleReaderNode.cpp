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
using namespace bytedance::bolt::shuffle::sparksql;

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
      row2ColConverter_(std::make_shared<ShuffleRowToColumnarConverter>(
          outputType_,
          pool())) {
  isValidityBuffer_.reserve(outputType_->size());
  for (size_t i = 0; i < outputType_->size(); ++i) {
    switch (outputType_->childAt(i)->kind()) {
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

  // must be same as BoltRuntime::decideBoltShuffleWriterType
  isRowBased_ = (!partitioningShortName_.compare("hash") ||
                 !partitioningShortName_.compare("rr") ||
                 !partitioningShortName_.compare("range")) &&
      ((shuffleWriterType_ == ShuffleWriterType::Adaptive &&
        numPartitions_ >= rowBasePartitionThreshold &&
        outputType_->size() >= rowBaseColumnNumThreshold) ||
       (shuffleWriterType_ == ShuffleWriterType::RowBased));
}

void SparkShuffleReader::init() {
  // Bolt operator should not alloc memory during construct, so init schema and
  // codec here
  schema_ = boltTypeToArrowSchema(outputType_, pool());
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
                outputType_,
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
      return output;
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
