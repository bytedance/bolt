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

#pragma once

#include <unordered_set>
#include "bolt/connectors/hive/PaimonEngine.h"
#include "bolt/connectors/hive/PaimonRowIterator.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/AggregateFunction.h"
namespace bytedance::bolt::connector::hive {

class PartialUpdateEngine : public PaimonEngine {
 public:
  PartialUpdateEngine(
      const bool ignoreDelete,
      const std::unordered_map<
          int,
          std::shared_ptr<connector::paimon::AggregateFunction>> aggregations,
      const std::vector<std::pair<std::vector<int>, std::vector<int>>>&
          sequenceGroups);
  virtual ~PartialUpdateEngine() = default;

  void setResult(RowVectorPtr result_) override;

  vector_size_t add(PaimonRowIteratorPtr iterator) override;

  vector_size_t finalizeCompletedGroups(
      const PaimonRowIteratorPtr& nextInput) override;

  vector_size_t finish() override;

 protected:
  void finalizeActiveRow();
  void startActiveRow();

  const bool ignoreDelete_;
  const std::
      unordered_map<int, std::shared_ptr<connector::paimon::AggregateFunction>>
          aggregations_;
  const std::vector<std::pair<std::vector<int>, std::vector<int>>>
      sequenceGroups_;
  std::unordered_set<int> sequenceGroupFields_;
  vector_size_t activeRowIndex_{0};
  bool hasActiveRow_{false};
  PaimonRowIterator lastPk_;
  std::vector<RowVectorPtr> lastSequenceGroupKeyValues;
};

} // namespace bytedance::bolt::connector::hive
