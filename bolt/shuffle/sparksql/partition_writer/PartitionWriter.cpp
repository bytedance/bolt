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

#include "bolt/shuffle/sparksql/partition_writer/PartitionWriter.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/partition_writer/LocalPartitionWriter.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/CelebornPartitionWriter.h"

using namespace bytedance::bolt::shuffle::sparksql;

std::unique_ptr<PartitionWriter> PartitionWriter::create(
    PartitionWriterOptions options,
    arrow::MemoryPool* pool) {
  if (options.partitionWriterType == PartitionWriterType::kLocal) {
    return std::make_unique<LocalPartitionWriter>(
        options.numPartitions,
        options,
        pool,
        options.dataFile,
        options.configuredDirs);
  } else if (options.partitionWriterType == PartitionWriterType::kCeleborn) {
    return std::make_unique<CelebornPartitionWriter>(
        options.numPartitions, options, pool, options.rssClient);
  } else {
    BOLT_FAIL(
        "Unsupported partition writer type: {}", options.partitionWriterType);
  }
  return nullptr;
}
