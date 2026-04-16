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

#include "bolt/connectors/hive/PaimonSplitReader.h"
#include "bolt/connectors/hive/PaimonConstants.h"
#include "bolt/connectors/hive/PaimonMiscHelpers.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateEngine.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldBoolAndAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldBoolOrAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldCollectAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldFirstNonNullValueAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldFirstValueAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldLastNonNullValueAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldLastValueAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldListaggAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldMaxAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldMinAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldPrimaryKeyAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldProductAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/FieldSumAgg.h"
#include "bolt/connectors/hive/paimon_merge_engines/DeduplicateEngine.h"
namespace bytedance::bolt::connector::hive {

PaimonSplitReader::PaimonSplitReader(
    std::vector<std::unique_ptr<SplitReader>> splitReaders,
    RowTypePtr readerOutputType,
    const std::vector<int>& primaryKeyIndices,
    const std::vector<int>& sequenceNumberIndices,
    int valueKindIndex,
    const std::vector<int>& valueIndices,
    const std::unordered_map<std::string, std::string>& tableParameters,
    const std::shared_ptr<io::IoStatistics> ioStats)
    : splitReaders_(std::move(splitReaders)),
      readerOutputType_(readerOutputType),
      primaryKeyIndices_(std::move(primaryKeyIndices)),
      sequenceNumberIndices_(sequenceNumberIndices),
      valueKindIndex_(valueKindIndex),
      valueIndices_(valueIndices),
      tableParameters_(tableParameters),
      mergeEngineType_(getMergeEngineType()),
      ignoreDelete_(getIgnoreDelete()),
      aggregateFunctions_(getAggregateFunctions()),
      aggregations_(getAggregations()),
      sequenceGroups_(getSequenceGroups()),
      mergeEngine_(getMergeEngine()),
      ioStats_(ioStats) {
  for (const auto& splitReader : splitReaders_) {
    auto iterator = getIterator(splitReader.get());
    if (iterator && iterator->primaryKeys->size() > 0) {
      heap.push(iterator);
    }
  }
}

RowTypePtr PaimonSplitReader::projectType(
    RowTypePtr rowType,
    const std::vector<vector_size_t>& indices) {
  std::vector<std::string> names;
  auto& colNames = rowType->names();

  std::vector<TypePtr> types;
  auto& colTypes = rowType->children();
  for (auto i : indices) {
    names.push_back(colNames[i]);
    types.push_back(colTypes[i]);
  }

  return std::make_shared<RowType>(std::move(names), std::move(types));
}

std::vector<VectorPtr> PaimonSplitReader::projectColumns(
    std::vector<VectorPtr>& columns,
    const std::vector<vector_size_t>& indices) {
  std::vector<VectorPtr> result;
  for (auto i : indices) {
    result.push_back(columns[i]);
  }
  return result;
}

RowVectorPtr PaimonSplitReader::projectVector(
    RowVectorPtr vect,
    const std::vector<vector_size_t>& indices) {
  auto* pool = vect->pool();
  auto type = vect->type();
  auto rowType = asRowType(vect->type());
  auto projectedRowTypePtr = projectType(rowType, indices);
  auto nulls = vect->nulls();
  auto length = vect->size();
  auto projectedChildren = projectColumns(vect->children(), indices);
  auto nullCount = vect->getNullCount();

  return std::make_shared<RowVector>(
      pool, projectedRowTypePtr, nulls, length, projectedChildren, nullCount);
}

PaimonRowIteratorPtr PaimonSplitReader::getIterator(SplitReader* rowReader) {
  VectorPtr result =
      BaseVector::create(readerOutputType_, 0, rowReader->pool());

  if (rowReader->next(paimon::kMAX_BATCH_SIZE, result)) {
    RowVectorPtr resultAsRowVect = std::static_pointer_cast<RowVector>(result);
    auto primaryKeys = projectVector(resultAsRowVect, primaryKeyIndices_);

    auto sequenceFields =
        projectVector(resultAsRowVect, sequenceNumberIndices_);

    auto valueKindVect = resultAsRowVect->childAt(valueKindIndex_);
    if (valueKindVect->isLazy()) {
      valueKindVect = valueKindVect->as<LazyVector>()->loadedVectorShared();
    }

    BaseVector::flattenVector(valueKindVect);
    auto valueKind = valueKindVect->asFlatVector<int8_t>()->rawValues();

    auto values = projectVector(resultAsRowVect, valueIndices_);

    std::vector<std::pair<RowVectorPtr, std::vector<int>>> sequenceGroups;
    sequenceGroups.reserve(sequenceGroups_.size());
    for (const auto& [groupKeyIndices, groupValueIndices] : sequenceGroups_) {
      auto groupKeys = projectVector(resultAsRowVect, groupKeyIndices);
      std::vector<int> combined = groupValueIndices;
      combined.insert(
          combined.end(), groupKeyIndices.begin(), groupKeyIndices.end());
      sequenceGroups.push_back(
          std::make_pair(groupKeys, getMappedIndices(combined, valueIndices_)));
    }

    return std::make_shared<PaimonRowIterator>(
        primaryKeys,
        sequenceFields,
        valueKindVect,
        valueKind,
        values,
        sequenceGroups,
        rowReader);
  } else {
    return nullptr;
  }
}

PaimonMergeEngineType PaimonSplitReader::getMergeEngineType() {
  auto it = tableParameters_.find(paimon::kMergeEngine);
  if (it == tableParameters_.end()) {
    return PaimonMergeEngineType::Deduplicate;
  }

  const std::string& engine = it->second;
  if (engine == paimon::kDeduplicateMergeEngine) {
    return PaimonMergeEngineType::Deduplicate;
  } else if (engine == paimon::kPartialUpdateMergeEngine) {
    return PaimonMergeEngineType::PartialUpdate;
  } else if (engine == paimon::kAggregateMergeEngine) {
    return PaimonMergeEngineType::Aggregate;
  } else if (engine == paimon::kFirstRowMergeEngine) {
    return PaimonMergeEngineType::FirstRow;
  } else {
    throw std::runtime_error("Unknown Paimon merge engine: " + engine);
  }
}

bool PaimonSplitReader::getIgnoreDelete() {
  if (mergeEngineType_ != PaimonMergeEngineType::Deduplicate) {
    return false;
  }

  auto it = tableParameters_.find(paimon::kIgnoreDelete);
  if (it == tableParameters_.end()) {
    return false;
  }

  const std::string& value = it->second;
  return value == "true";
}

std::unordered_map<std::string, int> PaimonSplitReader::getNameIdxMap() {
  std::unordered_map<std::string, int> result;
  for (int i = 0; i < readerOutputType_->size(); i++) {
    result[readerOutputType_->nameOf(i)] = i;
  }
  return result;
}

std::vector<std::pair<std::vector<int>, std::vector<int>>>
PaimonSplitReader::getSequenceGroups() {
  if (mergeEngineType_ != PaimonMergeEngineType::PartialUpdate) {
    return {};
  }

  auto nameIdxMap = getNameIdxMap();
  std::vector<std::pair<std::vector<int>, std::vector<int>>> result;
  auto sequenceGroupInfo =
      getSequenceGroupInfo(tableParameters_, readerOutputType_->names());
  for (auto& [keys, values] : sequenceGroupInfo) {
    auto keyIndices = getIndicesFromNames(keys, nameIdxMap);
    auto valueIndices = getIndicesFromNames(values, nameIdxMap);
    result.emplace_back(keyIndices, valueIndices);
  }

  return result;
}

std::optional<std::string>
PaimonSplitReader::getDefaultAggregateFunctionName() {
  for (const auto& [key, value] : tableParameters_) {
    if (key == connector::paimon::kPartialUpdateDefaultAggregateFunctionKey) {
      return value;
    }
  }
  return std::nullopt;
}

void PaimonSplitReader::populateDefaultAggregations() {
  auto defaultAggregateFunctionName = getDefaultAggregateFunctionName();

  if (defaultAggregateFunctionName.has_value()) {
    for (const auto& [groupKeyIndices, groupValueIndices] : sequenceGroups_) {
      for (const auto& i : groupValueIndices) {
        if (aggregations_.find(i) == aggregations_.end()) {
          aggregations_[i] =
              getAggregateFunction(defaultAggregateFunctionName.value(), i);
        }
      }
    }
  }
}

std::unordered_map<int, std::shared_ptr<connector::paimon::AggregateFunction>>
PaimonSplitReader::getAggregations() {
  if (mergeEngineType_ != PaimonMergeEngineType::PartialUpdate) {
    return {};
  }

  auto nameIdxMap = getNameIdxMap();
  std::unordered_map<int, std::shared_ptr<connector::paimon::AggregateFunction>>
      result;
  for (const auto& [key, value] : tableParameters_) {
    if (!startsWith(key, connector::paimon::kPartialUpdateKeyPrefix))
      continue;

    if (!endsWith(key, connector::paimon::kAggregateEngineKeyPostfix))
      continue;

    auto prefixSize = strlen(connector::paimon::kPartialUpdateKeyPrefix);
    auto postfixSize = strlen(connector::paimon::kAggregateEngineKeyPostfix);
    auto fieldName =
        key.substr(prefixSize, key.size() - prefixSize - postfixSize);
    auto fieldIdx = nameIdxMap[fieldName];
    result[fieldIdx] = getAggregateFunction(value, fieldIdx);
  }

  return result;
}

std::shared_ptr<PaimonEngine> PaimonSplitReader::getMergeEngine() {
  switch (mergeEngineType_) {
    case PaimonMergeEngineType::Deduplicate:
      return std::make_shared<DeduplicateEngine>(ignoreDelete_);
    case PaimonMergeEngineType::Aggregate:
      return std::make_shared<AggregateEngine>(aggregateFunctions_);
    case PaimonMergeEngineType::PartialUpdate:
      populateDefaultAggregations();
      return std::make_shared<PartialUpdateEngine>(
          ignoreDelete_, aggregations_, sequenceGroups_);
    case PaimonMergeEngineType::FirstRow:
    default:
      throw std::runtime_error("FirstRow engine is shouldn't come here.");
  }
}

std::string PaimonSplitReader::getListAggDelimiter(
    const std::string& fieldName) {
  auto key = connector::paimon::kAggregateEngineKeyPrefix + fieldName +
      connector::paimon::kAggregateListAggDelimiter;
  auto iter = tableParameters_.find(key);
  if (iter == tableParameters_.end())
    return ",";

  return iter->second;
}

std::shared_ptr<connector::paimon::AggregateFunction>
PaimonSplitReader::getAggregateFunction(
    const std::string& funcName,
    const int index) {
  auto fieldName = readerOutputType_->nameOf(index);
  auto type = readerOutputType_->childAt(index);
  if (funcName == connector::paimon::kAggregateSumName) {
    return connector::paimon::createFieldSumAgg(type);
  } else if (funcName == connector::paimon::kAggregateProductName) {
    return connector::paimon::createFieldProductAgg(type);
  } else if (funcName == connector::paimon::kAggregateBoolAndName) {
    return connector::paimon::createFieldBoolAndAgg(type);
  } else if (funcName == connector::paimon::kAggregateBoolOrName) {
    return connector::paimon::createFieldBoolOrAgg(type);
  } else if (funcName == connector::paimon::kAggregateListAggName) {
    auto delimiter = getListAggDelimiter(fieldName);
    return connector::paimon::createFieldListaggAgg(delimiter, type);
  } else if (funcName == connector::paimon::kAggregateFirstNonNullValueName) {
    return connector::paimon::createFieldFirstNonNullValueAgg();
  } else if (funcName == connector::paimon::kAggregateFirstValueName) {
    return connector::paimon::createFieldFirstValueAgg();
  } else if (funcName == connector::paimon::kAggregateLastNonNullValueName) {
    return connector::paimon::createFieldLastNonNullValueAgg();
  } else if (funcName == connector::paimon::kAggregateLastValueName) {
    return connector::paimon::createFieldLastValueAgg();
  } else if (funcName == connector::paimon::kAggregateMinName) {
    return connector::paimon::createFieldMinAgg();
  } else if (funcName == connector::paimon::kAggregateMaxName) {
    return connector::paimon::createFieldMaxAgg();
  } else if (funcName == connector::paimon::kAggregateCollectName) {
    auto distinctKey =
        std::string(connector::paimon::kAggregateEngineKeyPrefix) + fieldName +
        connector::paimon::kAggregateCollectDistinct;
    bool distinct = false;
    if (tableParameters_.find(distinctKey) != tableParameters_.end()) {
      distinct = tableParameters_.at(distinctKey) == "true";
    }
    return connector::paimon::createFieldCollectAgg(type, distinct);
  } else {
    BOLT_NYI(funcName + " function is not yet implemented.");
  }
}

std::vector<std::shared_ptr<connector::paimon::AggregateFunction>>
PaimonSplitReader::getAggregateFunctions() {
  if (mergeEngineType_ != PaimonMergeEngineType::Aggregate) {
    return {};
  }

  std::vector<std::shared_ptr<connector::paimon::AggregateFunction>> results;
  for (int valueIndex : valueIndices_) {
    if (std::find(
            primaryKeyIndices_.begin(), primaryKeyIndices_.end(), valueIndex) !=
        primaryKeyIndices_.end()) {
      results.push_back(connector::paimon::createFieldPrimaryKeyAggregator());
      continue;
    }

    auto fieldName = readerOutputType_->nameOf(valueIndex);
    auto key = connector::paimon::kAggregateEngineKeyPrefix + fieldName +
        connector::paimon::kAggregateEngineKeyPostfix;
    auto iter = tableParameters_.find(key);
    if (iter == tableParameters_.end()) {
      results.push_back(connector::paimon::createFieldLastNonNullValueAgg());
      continue;
    }

    const std::string& value = iter->second;
    results.push_back(getAggregateFunction(value, valueIndex));
  }

  return results;
}

uint64_t PaimonSplitReader::next(int64_t size, VectorPtr& output) {
  output = BaseVector::create(output->type(), 0, output->pool());

  mergeEngine_->setResult(std::dynamic_pointer_cast<RowVector>(output));

  auto mergetStartTime = getCurrentTimeMicro();
  while (true) {
    auto iterator = heap.empty() ? nullptr : heap.top();
    auto rowCnt = mergeEngine_->finalizeCompletedGroups(iterator);

    if (rowCnt >= size || !iterator) {
      auto mergeTime = (getCurrentTimeMicro() - mergetStartTime) * 1000;
      ioStats_->incTotalMergeTime(mergeTime);
      return rowCnt;
    }

    VLOG(2) << "Values:" << iterator->values->toString(iterator->rowIndex)
            << "   Seq:"
            << iterator->sequenceFields->toString(iterator->rowIndex);
    heap.pop();

    mergeEngine_->add(iterator);

    if (iterator->next()) {
      heap.push(iterator);
    } else {
      auto nextBatchIterator = getIterator(iterator->reader);
      if (nextBatchIterator)
        heap.push(nextBatchIterator);
    }
  }
}

bool PaimonSplitReader::allPrefetchIssued() const {
  bool result = true;
  for (const auto& splitReader : splitReaders_) {
    result = result && splitReader->allPrefetchIssued();
  }
  return result;
}

bool PaimonSplitReader::emptySplit() const {
  bool result = true;
  for (const auto& splitReader : splitReaders_) {
    result = result && splitReader->emptySplit();
  }
  return result;
}

void PaimonSplitReader::resetFilterCaches() {
  for (const auto& splitReader : splitReaders_) {
    splitReader->resetFilterCaches();
  }
}

int64_t PaimonSplitReader::estimatedRowSize() const {
  return splitReaders_[0]->estimatedRowSize();
}

void PaimonSplitReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  for (const auto& splitReader : splitReaders_) {
    splitReader->updateRuntimeStats(stats);
  }
}

void PaimonSplitReader::resetSplit() {
  for (const auto& splitReader : splitReaders_) {
    splitReader->resetSplit();
  }
}
} // namespace bytedance::bolt::connector::hive
