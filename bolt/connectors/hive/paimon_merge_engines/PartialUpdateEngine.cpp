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

#include "bolt/connectors/hive/paimon_merge_engines/PartialUpdateEngine.h"
namespace bytedance::bolt::connector::hive {

PartialUpdateEngine::PartialUpdateEngine(
    const bool ignoreDelete,
    const std::unordered_map<
        int,
        std::shared_ptr<connector::paimon::AggregateFunction>> aggregations,
    const std::vector<std::pair<std::vector<int>, std::vector<int>>>&
        sequenceGroups)
    : ignoreDelete_(ignoreDelete),
      aggregations_(std::move(aggregations)),
      sequenceGroups_(std::move(sequenceGroups)) {
  sequenceGroupFields_ = {};
  for (auto group : sequenceGroups_) {
    sequenceGroupFields_.insert(group.first.begin(), group.first.end());
    sequenceGroupFields_.insert(group.second.begin(), group.second.end());
  }

  lastSequenceGroupKeyValues =
      std::vector<RowVectorPtr>(sequenceGroups_.size(), nullptr);
}

vector_size_t PartialUpdateEngine::add(PaimonRowIteratorPtr iterator) {
  if (!lastPk_.primaryKeys || !lastPk_.pkEqual(iterator)) {
    if (result->size() > 0) {
      for (const auto& [idx, aggr] : aggregations_) {
        auto dest = result->childAt(idx);
        aggr->appendResult(dest);
      }
    }
    result->resize(result->size() + 1);
    const auto newRowIdx = result->size() - 1;
    for (int i = 0; i < result->childrenSize(); i++) {
      result->childAt(i)->setNull(newRowIdx, true);
    }
    lastSequenceGroupKeyValues =
        std::vector<RowVectorPtr>(sequenceGroups_.size(), nullptr);
  }

  VLOG(2) << "Adding Iter:"
          << iterator->primaryKeys->toString(iterator->rowIndex) << "-->"
          << iterator->values->toString(iterator->rowIndex)
          << "  Seq:" << iterator->sequenceFields->toString(iterator->rowIndex);
  VLOG(2) << "  Sequence Group:" << std::endl;
  for (size_t i = 0; i < iterator->sequenceGroups.size(); i++) {
    VLOG(2) << "\t\t\t\t"
            << iterator->sequenceGroups[i].first->toString(iterator->rowIndex)
            << std::endl;
  }

  int destIdx = result->size() - 1;
  for (int i = 0; i < iterator->values->childrenSize(); i++) {
    if (sequenceGroupFields_.find(i) == sequenceGroupFields_.end()) {
      auto src = iterator->values->childAt(i);
      if (!src->isNullAt(iterator->rowIndex)) {
        auto dest = result->childAt(i);
        dest->copy(src.get(), destIdx, iterator->rowIndex, 1);
      }
    }
  }

  for (size_t i = 0; i < iterator->sequenceGroups.size(); i++) {
    auto& lastSequenceGroupValue = lastSequenceGroupKeyValues[i];
    auto key = iterator->sequenceGroups[i].first;
    if (!lastSequenceGroupValue ||
        lastSequenceGroupValue->compare(
            key.get(), 0, iterator->rowIndex, CompareFlags()) < 0) {
      for (const auto& idx : iterator->sequenceGroups[i].second) {
        if (aggregations_.find(idx) == aggregations_.end()) {
          auto dest = result->childAt(idx);
          auto src = iterator->values->childAt(idx);
          dest->copy(src.get(), destIdx, iterator->rowIndex, 1);
        } else {
          auto& child = iterator->values->childAt(idx);
          BaseVector::flattenVector(child);
          aggregations_.at(idx)->add(child, iterator->rowIndex);
        }
      }

      auto key = iterator->sequenceGroups[i].first;
      if (!lastSequenceGroupValue) {
        lastSequenceGroupValue = std::static_pointer_cast<RowVector>(
            BaseVector::create(key->type(), 1, key->pool()));
      }

      lastSequenceGroupValue->copy(key.get(), 0, iterator->rowIndex, 1);
    }
  }

  lastPk_ = *iterator;

  VLOG(2) << "Result:"
          << "-->" << result->toString(result->size() - 1);
  VLOG(2) << "  Sequence group key:";
  for (int i = 0; i < lastSequenceGroupKeyValues.size(); i++) {
    VLOG(2) << "\t\t\t\t"
            << (lastSequenceGroupKeyValues[i]
                    ? lastSequenceGroupKeyValues[i]->toString(0)
                    : "null")
            << std::endl;
  }

  return result->size();
}

vector_size_t PartialUpdateEngine::finish() {
  if (result->size() > 0) {
    for (const auto& [idx, aggr] : aggregations_) {
      auto dest = result->childAt(idx);
      aggr->appendResult(dest);
    }
  }

  return result->size();
}

} // namespace bytedance::bolt::connector::hive
