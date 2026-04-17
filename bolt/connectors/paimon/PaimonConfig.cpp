/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/connectors/paimon/PaimonConfig.h"

namespace bytedance::bolt::connector::paimon {

PaimonConfig::PaimonConfig(
    std::shared_ptr<const config::ConfigBase> config)
    : config_(config ? std::move(config)
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

} // namespace bytedance::bolt::connector::paimon
