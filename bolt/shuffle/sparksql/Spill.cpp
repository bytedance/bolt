/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

#include <iostream>

#include "bolt/shuffle/sparksql/Spill.h"
namespace bytedance::bolt::shuffle::sparksql {

Spill::Spill(
    Spill::SpillType type,
    uint32_t numPartitions,
    const std::string& spillFile)
    : type_(type), numPartitions_(numPartitions), spillFile_(spillFile) {}

Spill::~Spill() {
  if (is_) {
    (void)is_->Close();
  }
}

bool Spill::hasNextPayload(uint32_t partitionId) {
  return !partitionPayloads_.empty() &&
      partitionPayloads_.front().partitionId == partitionId;
}

std::unique_ptr<Payload> Spill::nextPayload(uint32_t partitionId) {
  openSpillFile();
  if (!hasNextPayload(partitionId)) {
    return nullptr;
  }
  auto payload = std::move(partitionPayloads_.front().payload);
  partitionPayloads_.pop_front();
  return payload;
}

void Spill::insertPayload(
    uint32_t partitionId,
    Payload::Type payloadType,
    uint32_t numRows,
    const std::vector<bool>* isValidityBuffer,
    uint64_t rawSize,
    arrow::MemoryPool* pool,
    Codec* codec) {
  // TODO: Add compression threshold.
  switch (payloadType) {
    case Payload::Type::kUncompressed:
    case Payload::Type::kToBeCompressed:
      partitionPayloads_.push_back(
          {partitionId,
           std::make_unique<UncompressedDiskBlockPayload>(
               payloadType,
               numRows,
               isValidityBuffer,
               rawIs_,
               rawSize,
               pool,
               codec)});
      break;
    case Payload::Type::kCompressed:
      partitionPayloads_.push_back(
          {partitionId,
           std::make_unique<CompressedDiskBlockPayload>(
               numRows, isValidityBuffer, rawIs_, rawSize, pool)});
      break;
    default:
      BOLT_CHECK(false, "unexpected type " + std::to_string(payloadType));
      break;
  }
}

void Spill::insertPayload(
    uint32_t partitionId,
    uint32_t numRows,
    uint64_t rawSize) {
  partitionPayloads_.push_back(
      {partitionId,
       std::make_unique<CompressedDiskRowBlockPayload>(
           numRows, rawIs_, rawSize)});
}

void Spill::openSpillFile() {
  if (!is_) {
    auto result = arrow::io::MemoryMappedFile::Open(
        spillFile_, arrow::io::FileMode::READ);
    BOLT_CHECK(result.ok(), "Failed to open spill file: {}", spillFile_);
    is_ = result.ValueOrDie();
    rawIs_ = is_.get();
  }
}

Spill::SpillType Spill::type() const {
  return type_;
}

const std::string& Spill::spillFile() const {
  return spillFile_;
}
} // namespace bytedance::bolt::shuffle::sparksql
