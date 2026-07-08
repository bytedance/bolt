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
#include "bolt/common/time/Timer.h"
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
      shuffleBufferSize_(shuffleReaderOptions_.shuffleBufferSize),
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

  // must be same as BoltShuffleWriter::decideBoltShuffleWriterType
  auto partitioning = toPartitioning(partitioningShortName_);
  isRowBased_ = supportAdaptiveShuffleWriter(partitioning) &&
      ((shuffleWriterType_ == ShuffleWriterType::Adaptive &&
        numPartitions_ >= rowBasePartitionThreshold &&
        outputType_->size() >= rowBaseColumnNumThreshold) ||
       (shuffleWriterType_ == ShuffleWriterType::RowBased));
  reuseBufferedInputStream_ = shuffleReaderOptions_.reuseBufferedInputStream;
}

void SparkShuffleReader::init() {
  // Bolt operator should not alloc memory during construct, so init schema and
  // codec here
  schema_ = boltTypeToArrowSchema(outputType_, pool());
  zstdCodec_ = std::make_shared<AdaptiveParallelZstdCodec>(
      1 /*not used*/, false, arrowPool_.get());
}

bytedance::bolt::RowVectorPtr SparkShuffleReader::getOutput() {
  NanosecondTimer timerRead(&totalReadTime_);
  std::call_once(initFlag_, &SparkShuffleReader::init, this);
  if (finished_) {
    return nullptr;
  }

  if (reuseBufferedInputStream_) {
    // Reuse a single BufferedInputStream by chaining all reader streams into
    // one continuous stream behind a single deserializer.
    if (!columnarBatchDeserializer_) {
      NanosecondTimer timer(&deserializerCreateTime_);
      auto chainedStream = std::make_shared<ChainedReaderStream>(
          readerStreamIterator_, arrowPool_.get());
      columnarBatchDeserializer_ =
          std::make_unique<BoltColumnarBatchDeserializer>(
              std::move(chainedStream),
              schema_,
              codec_,
              outputType_,
              batchSize_,
              shuffleBatchByteSize_,
              shuffleBufferSize_,
              arrowPool_.get(),
              pool(),
              &isValidityBuffer_,
              hasComplexType_,
              deserializeTime_,
              decompressTime_,
              mergeTime_,
              isRowBased_,
              zstdCodec_.get(),
              rowBufferPool_.get(),
              row2ColConverter_.get());
    }

    auto output = columnarBatchDeserializer_->next();
    if (!output) {
      finished_ = true;
    }
    return output;
  }

  // Legacy path: create a new deserializer (and BufferedInputStream) per
  // stream.
  while (true) {
    if (!columnarBatchDeserializer_) {
      auto in = readerStreamIterator_->nextStream(arrowPool_.get());
      if (in) {
        NanosecondTimer timer(&deserializerCreateTime_);
        columnarBatchDeserializer_ =
            std::make_unique<BoltColumnarBatchDeserializer>(
                std::move(in),
                schema_,
                codec_,
                outputType_,
                batchSize_,
                shuffleBatchByteSize_,
                shuffleBufferSize_,
                arrowPool_.get(),
                pool(),
                &isValidityBuffer_,
                hasComplexType_,
                deserializeTime_,
                decompressTime_,
                mergeTime_,
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
      NanosecondTimer timer(&deserializerDestroyTime_);
      columnarBatchDeserializer_ = nullptr;
    }
  }
}

void SparkShuffleReader::close() {
  // Account for the destroy overhead of the last deserializer that is still
  // alive (e.g. the single reused deserializer in the reuse path).
  if (columnarBatchDeserializer_) {
    NanosecondTimer timer(&deserializerDestroyTime_);
    columnarBatchDeserializer_ = nullptr;
  }

  {
    auto stats = this->stats().rlock();
    readerStreamIterator_->updateMetrics(
        stats->outputPositions,
        stats->outputVectors,
        decompressTime_,
        deserializeTime_,
        deserializerCreateTime_,
        deserializerDestroyTime_,
        mergeTime_,
        totalReadTime_);
  }

  if (readerStreamIterator_) {
    readerStreamIterator_->close();
    readerStreamIterator_ = nullptr;
  }
  bytedance::bolt::exec::SourceOperator::close();
}
