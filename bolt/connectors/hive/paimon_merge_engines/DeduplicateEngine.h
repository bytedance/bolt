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

#include "bolt/connectors/hive/PaimonEngine.h"
#include "bolt/connectors/hive/PaimonRowIterator.h"
namespace bytedance::bolt::connector::hive {

class DeduplicateEngine : public PaimonEngine {
 public:
  PaimonRowIterator last;
  const bool ignoreDelete;

  DeduplicateEngine(bool ignoreDelete_);

  virtual ~DeduplicateEngine() = default;

  vector_size_t add(PaimonRowIteratorPtr iterator) override;

  vector_size_t finalizeCompletedGroups(
      const PaimonRowIteratorPtr& nextInput) override;

  vector_size_t finish() override;
};

} // namespace bytedance::bolt::connector::hive
