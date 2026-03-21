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

#include "bolt/connectors/hive/LoserTree.h"

#include <algorithm>

namespace bytedance::bolt::connector::hive {
void LoserTree::buildTree() {
  // initialize to sentinels
  std::fill(loserTree_.begin(), loserTree_.end(), EMPTY);

  // build bottom up
  for (size_t i = 0; i < size_; i++) {
    play(i);
  }
}

void LoserTree::play(int s) {
  int pos = (s + size_) / 2;
  while (pos > 0) {
    if (loserTree_[pos] == EMPTY) {
      VLOG(2) << "Emplacing empty by " << s << " at position " << pos;
      loserTree_[pos] = s;
      return;
    }

    if (leaves_[s]->compare(*leaves_[loserTree_[pos]]) > 0) {
      VLOG(2) << "Compared " << s << " with " << loserTree_[pos]
              << " and swapped.";
      std::swap(s, loserTree_[pos]);
    } else {
      VLOG(2) << "Compared " << s << " with " << loserTree_[pos]
              << " and not swapped." << std::endl;
    }
    pos = pos >> 1;
  }

  loserTree_[0] = s;
}

bool compare(const PaimonRowIteratorPtr& a, const PaimonRowIteratorPtr& other) {
  return a->primaryKeys
             ->compare(
                 other->primaryKeys.get(),
                 a->rowIndex,
                 other->rowIndex,
                 CompareFlags())
             .value() < 0;
}

void LoserTree::adjust(int winnerIdx) {
  auto pos = (winnerIdx + size_) / 2;
  while (pos > 0) {
    if (!leaves_[winnerIdx]) {
      winnerIdx = loserTree_[pos];
    } else if (!leaves_[loserTree_[pos]]) {
      VLOG(2) << "Compared " << winnerIdx << " with " << loserTree_[pos]
              << "(NULL) and swapped.";
    } else if (leaves_[winnerIdx]->compare(*leaves_[loserTree_[pos]]) > 0) {
      VLOG(2) << "Compared " << winnerIdx << " with " << loserTree_[pos]
              << " and swapped.";
      std::swap(winnerIdx, loserTree_[pos]);
    } else {
      VLOG(2) << "Compared " << winnerIdx << " with " << loserTree_[pos]
              << " and not swapped.";
    }

    pos = pos >> 1;
  }

  loserTree_[0] = winnerIdx;
}

} // namespace bytedance::bolt::connector::hive
