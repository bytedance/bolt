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

#include "bolt/connectors/hive/paimon_merge_engines/AggregateEngine.h"
namespace bytedance::bolt::connector::hive {

AggregateEngine::AggregateEngine(
    const std::vector<std::shared_ptr<connector::paimon::AggregateFunction>>&
        aggregateFunctions)
    : aggregateFunctions_(aggregateFunctions) {}

vector_size_t AggregateEngine::add(PaimonRowIteratorPtr iterator) {
  if (lastPk_.primaryKeys && !lastPk_.pkEqual(iterator)) {
    finalizeCompletedGroups(iterator);
  }

  lastPk_ = *iterator;

  for (auto i = 0; i < aggregateFunctions_.size(); i++) {
    aggregateFunctions_[i]->add(
        iterator->values->childAt(i), iterator->rowIndex);
  }

  return result->size();
}

vector_size_t AggregateEngine::finalizeCompletedGroups(
    const PaimonRowIteratorPtr& nextInput) {
  if (lastPk_.primaryKeys && (!nextInput || !lastPk_.pkEqual(nextInput))) {
    result->resize(result->size() + 1);
    for (auto i = 0; i < aggregateFunctions_.size(); i++) {
      auto dest = result->childAt(i);
      aggregateFunctions_[i]->appendResult(dest);
    }
    lastPk_.primaryKeys = nullptr;
  }

  return result->size();
}

vector_size_t AggregateEngine::finish() {
  return finalizeCompletedGroups(nullptr);
}

} // namespace bytedance::bolt::connector::hive
