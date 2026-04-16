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

#include "bolt/connectors/Connector.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

struct ArrowMemoryTable {
  // If inColumns_ is true, each arrow array/schema stands for one column
  // If inColumns_ is false, there are only one pair of struct-typed arrow
  // array/schema in the vectors, representing the whole table
  std::vector<ArrowSchema> arrowSchemas_;
  std::vector<ArrowArray> arrowArrays_;
  bool inColumns_;

  ArrowMemoryTable(
      std::vector<ArrowSchema> arrowSchemas,
      std::vector<ArrowArray> arrowArrays,
      bool inColumns)
      : arrowSchemas_(arrowSchemas),
        arrowArrays_(arrowArrays),
        inColumns_(inColumns) {}
};
namespace bytedance::bolt::connector::arrow {

class ArrowMemoryConnectorSplit : public ConnectorSplit {
 private:
  std::optional<ArrowMemoryTable> arrowTable_;
  bool finished_;

 public:
  ArrowMemoryConnectorSplit(const std::string& connectorId)
      : ConnectorSplit(connectorId), finished_(false){};

  ArrowMemoryTable getArrowSplitData() {
    BOLT_CHECK(arrowTable_.has_value(), "Arrow Table should not be empty");
    return arrowTable_.value();
  }

  void addArrowSplitData(
      std::vector<ArrowSchema> arrowSchemas,
      std::vector<ArrowArray> arrowArrays,
      bool inColumns) {
    BOLT_CHECK_EQ(
        arrowSchemas.size(),
        arrowArrays.size(),
        "The Arrow Array and Schema must have the same number of columns");

    // inColumns is false. There is only one pair of struct-typed input Arrow
    // Array and Schema, representing the whole table.
    if (!inColumns) {
      BOLT_CHECK_EQ(
          arrowArrays.size(),
          1,
          "There should be only one pair of struct-typed arrow schema/array");
      BOLT_CHECK_EQ(
          std::string(arrowSchemas[0].format),
          "+s",
          "Arrow Table must be in struct type");
    }
    arrowTable_ = ArrowMemoryTable(arrowSchemas, arrowArrays, inColumns);
  }

  void setArrowSplitFinished() {
    finished_ = true;
  }

  bool getArrowSplitFinishStatus() {
    return finished_;
  }

  int64_t splitSizeBytes() const override {
    return 0;
  }

  ~ArrowMemoryConnectorSplit() {
    if (!finished_ && arrowTable_.has_value()) {
      int columnSize = arrowTable_.value().arrowSchemas_.size();
      for (int i = 0; i < columnSize; ++i) {
        auto arrowSchema = &arrowTable_.value().arrowSchemas_[i];
        if (arrowSchema != nullptr && arrowSchema->release != nullptr) {
          arrowSchema->release(arrowSchema);
        }
        auto arrowArray = &arrowTable_.value().arrowArrays_[i];
        if (arrowArray != nullptr && arrowArray->release != nullptr) {
          arrowArray->release(arrowArray);
        }
      }
    }
  }
};

} // namespace bytedance::bolt::connector::arrow
