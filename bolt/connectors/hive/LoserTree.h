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

#include "bolt/connectors/hive/PaimonRowIterator.h"

namespace bytedance::bolt::connector::hive {

bool compare(const PaimonRowIteratorPtr& a, const PaimonRowIteratorPtr& other);

class LoserTree {
 public:
  // the loser tree spot is not filled, i.e not initialized
  const int EMPTY = -1;

  void buildTree();

  explicit LoserTree(std::vector<PaimonRowIteratorPtr>& leaves)
      : leaves_(leaves), size_(leaves.size()) {
    loserTree_.resize(size_, EMPTY);
    BOLT_CHECK_GT(size_, 0);

    buildTree();
  }

  void adjust(int winnerIdx);

  bool done() {
    return !leaves_[loserTree_[0]];
  }

  PaimonRowIteratorPtr winner() {
    auto winnerIdx = loserTree_[0];
    BOLT_CHECK_NE(winnerIdx, EMPTY);
    return leaves_[winnerIdx];
  }

  int winnerIdx() {
    auto winnerIdx = loserTree_[0];
    BOLT_CHECK_NE(winnerIdx, EMPTY);
    return winnerIdx;
  }

  void updateLeaf(int winnerIdx, PaimonRowIteratorPtr iterator) {
    leaves_[winnerIdx] = iterator;
  }

 private:
  void play(int s);
  std::vector<PaimonRowIteratorPtr> leaves_;
  std::vector<int> loserTree_;
  const int size_;
};

} // namespace bytedance::bolt::connector::hive
