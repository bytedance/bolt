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

#include "bolt/connectors/hive/paimon_merge_engines/DeduplicateEngine.h"
#include "bolt/connectors/hive/paimon_merge_engines/PaimonRowKind.h"
namespace bytedance::bolt::connector::hive {

DeduplicateEngine::DeduplicateEngine(bool ignoreDelete_)
    : ignoreDelete(ignoreDelete_) {}

vector_size_t DeduplicateEngine::add(PaimonRowIteratorPtr iterator) {
  assert(iterator);

  // no data in it
  if (iterator->primaryKeys->size() == 0) {
    return result->size();
  }

  if (ignoreDelete && iterator->isRetract())
    return result->size();

  if (last.primaryKeys && !last.pkEqual(iterator)) {
    finalizeCompletedGroups(iterator);
  }

  if (last.primaryKeys) {
    VLOG(2) << "Adding Last:"
            << "-->" << last.values->toString(last.rowIndex)
            << "  Seq:" << last.sequenceFields->toString(last.rowIndex);
  }
  VLOG(2) << "Adding Iter:"
          << "-->" << iterator->values->toString(iterator->rowIndex)
          << "  Seq:" << iterator->sequenceFields->toString(iterator->rowIndex);

  if (!last.primaryKeys) {
    last = *iterator;
  } else if (last.sequenceFields
                 ->compare(
                     iterator->sequenceFields.get(),
                     last.rowIndex,
                     iterator->rowIndex,
                     CompareFlags())
                 .value()) {
    last = *iterator;
  }

  return result->size();
}

vector_size_t DeduplicateEngine::finalizeCompletedGroups(
    const PaimonRowIteratorPtr& nextInput) {
  if (last.primaryKeys && (!nextInput || !last.pkEqual(nextInput))) {
    if (last.isAdd()) {
      append(result, last);
    }
    last.primaryKeys = nullptr;
  }

  return result->size();
}

vector_size_t DeduplicateEngine::finish() {
  return finalizeCompletedGroups(nullptr);
}

} // namespace bytedance::bolt::connector::hive
