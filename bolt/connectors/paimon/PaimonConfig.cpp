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

#include "bolt/connectors/paimon/PaimonConfig.h"

namespace bytedance::bolt::connector::paimon {

PaimonConfig::PaimonConfig(std::shared_ptr<const config::ConfigBase> config)
    : config_(
          config ? std::move(config)
                 : std::make_shared<const config::ConfigBase>(
                       std::unordered_map<std::string, std::string>{})) {}

// ---------------------------------------------------------------------------
// Batch / Row Reading
// ---------------------------------------------------------------------------

int32_t PaimonConfig::readBatchSize() const {
  return config_->get<int32_t>(kReadBatchSize, 16384);
}

bool PaimonConfig::multiThreadRowToBatch() const {
  return config_->get<bool>(kMultiThreadRowToBatch, false);
}

uint32_t PaimonConfig::rowToBatchThreadNumber() const {
  return config_->get<uint32_t>(kRowToBatchThreadNum, 1);
}

// ---------------------------------------------------------------------------
// Prefetch & I/O
// ---------------------------------------------------------------------------

bool PaimonConfig::prefetchEnabled() const {
  return config_->get<bool>(kPrefetchEnabled, false);
}

uint32_t PaimonConfig::prefetchBatchCount() const {
  return config_->get<uint32_t>(kPrefetchBatchCount, 600);
}

uint32_t PaimonConfig::prefetchMaxParallel() const {
  return config_->get<uint32_t>(kPrefetchMaxParallel, 3);
}

// ---------------------------------------------------------------------------
// Filter Pushdown
// ---------------------------------------------------------------------------

bool PaimonConfig::predicateFilterEnabled() const {
  return config_->get<bool>(kPredicateFilterEnabled, true);
}

// ---------------------------------------------------------------------------
// I/O Tuning
// ---------------------------------------------------------------------------

uint64_t PaimonConfig::naturalReadSize() const {
  return config_->get<uint64_t>(kNaturalReadSize, 32ULL << 20); // 32 MB
}

bool PaimonConfig::coalesceReads() const {
  return config_->get<bool>(kCoalesceReads, true);
}

// ---------------------------------------------------------------------------
// Read Semantics
// ---------------------------------------------------------------------------

uint8_t PaimonConfig::readTimestampUnit() const {
  return config_->get<uint8_t>(kReadTimestampUnit, 3 /*milli*/);
}

} // namespace bytedance::bolt::connector::paimon
