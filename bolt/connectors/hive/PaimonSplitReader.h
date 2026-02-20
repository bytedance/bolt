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

#include "bolt/connectors/hive/HiveSplitReaderBase.h"
#include "bolt/connectors/hive/LoserTree.h"
#include "bolt/connectors/hive/PaimonConstants.h"
#include "bolt/connectors/hive/PaimonEngine.h"
#include "bolt/connectors/hive/PaimonMergeEngineType.h"
#include "bolt/connectors/hive/PaimonRowIterator.h"
#include "bolt/connectors/hive/SplitReader.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/AggregateFunction.h"
#include "bolt/connectors/hive/paimon_merge_engines/PartialUpdateEngine.h"

namespace bytedance::bolt {
class BaseVector;
using VectorPtr = std::shared_ptr<BaseVector>;
} // namespace bytedance::bolt

namespace bytedance::bolt::connector::hive {

class PaimonSplitReader : public HiveSplitReaderBase {
 public:
  PaimonSplitReader(
      std::vector<std::unique_ptr<SplitReader>> splitReaders,
      RowTypePtr readerOutputType,
      const std::vector<int>& primaryKeyIndices,
      const std::vector<int>& sequenceNumberIndices,
      int valueKindIndex,
      const std::vector<int>& valueIndices,
      const std::unordered_map<std::string, std::string>& tableParameters,
      const std::shared_ptr<io::IoStatistics> ioStats);

  virtual uint64_t next(int64_t size, VectorPtr& output) override;

  virtual bool allPrefetchIssued() const override;

  virtual bool emptySplit() const override;

  virtual void resetFilterCaches() override;

  virtual int64_t estimatedRowSize() const override;

  virtual void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  virtual void resetSplit() override;

  PaimonRowIteratorPtr getIterator(SplitReader* rowReader);

  RowTypePtr projectType(
      RowTypePtr rowType,
      const std::vector<vector_size_t>& indices);

  std::vector<VectorPtr> projectColumns(
      std::vector<VectorPtr>& columns,
      const std::vector<vector_size_t>& indices);

  RowVectorPtr projectVector(
      RowVectorPtr vect,
      const std::vector<vector_size_t>& indices);

  PaimonMergeEngineType getMergeEngineType();

  bool getIgnoreDelete();

  std::shared_ptr<PaimonEngine> getMergeEngine();

  std::shared_ptr<connector::paimon::AggregateFunction> getAggregateFunction(
      const std::string& funcName,
      const int index);

  std::vector<std::shared_ptr<connector::paimon::AggregateFunction>>
  getAggregateFunctions();

  std::string getListAggDelimiter(const std::string& fieldName);

  std::unordered_map<int, std::shared_ptr<connector::paimon::AggregateFunction>>
  getAggregations();

  std::optional<std::string> getDefaultAggregateFunctionName();

  void populateDefaultAggregations();

  std::vector<std::pair<std::vector<int>, std::vector<int>>>
  getSequenceGroups();

  std::unordered_map<std::string, int> getNameIdxMap();

 protected:
  std::vector<std::unique_ptr<SplitReader>> splitReaders_;
  RowTypePtr readerOutputType_;
  const std::vector<int> primaryKeyIndices_;
  const std::vector<int> sequenceNumberIndices_;
  const int valueKindIndex_;
  const std::vector<int> valueIndices_;
  const std::unordered_map<std::string, std::string>& tableParameters_;
  const PaimonMergeEngineType mergeEngineType_;
  const bool ignoreDelete_;
  const std::vector<std::shared_ptr<connector::paimon::AggregateFunction>>
      aggregateFunctions_;
  std::unordered_map<int, std::shared_ptr<connector::paimon::AggregateFunction>>
      aggregations_;
  const std::vector<std::pair<std::vector<int>, std::vector<int>>>
      sequenceGroups_;
  const std::shared_ptr<PaimonEngine> mergeEngine_;
  std::priority_queue<
      PaimonRowIteratorPtr,
      std::vector<PaimonRowIteratorPtr>,
      PaimonRowIteratorCompare>
      heap;

  std::shared_ptr<LoserTree> loserTree_;

  const std::shared_ptr<io::IoStatistics> ioStats_;
};

} // namespace bytedance::bolt::connector::hive
