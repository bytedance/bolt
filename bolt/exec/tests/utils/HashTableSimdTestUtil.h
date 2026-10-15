/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <string>
#include <unordered_map>
#include <vector>

#include "bolt/exec/tests/utils/AssertQueryBuilder.h"

namespace bytedance::bolt::exec::test {

inline void assertQueryResultsEqualWithSimdHashTable(
    const core::PlanNodePtr& plan,
    memory::MemoryPool* pool,
    const char* operatorName,
    const std::unordered_map<std::string, std::string>& extraConfigs = {}) {
  auto run = [&](bool enableSimd) {
    AssertQueryBuilder builder(plan);
    builder.config(
        core::QueryConfig::kSimdHashTableEnabled,
        enableSimd ? "true" : "false");
    for (const auto& [name, value] : extraConfigs) {
      builder.config(name, value);
    }
    return builder.copyResults(pool);
  };

  auto scalarResult = run(false);
  auto simdResult = run(true);
  ASSERT_TRUE(test::assertEqualResults(
      std::vector<RowVectorPtr>{simdResult},
      std::vector<RowVectorPtr>{scalarResult}))
      << "SIMD vs scalar " << operatorName
      << " results differ.\nSIMD rows=" << simdResult->size()
      << " scalar rows=" << scalarResult->size();
}

inline std::vector<std::string> hashTableSimdVarcharValues() {
  return {
      "",
      "a",
      "abcd",
      "abcdefgh",
      std::string(11, 'k'),
      std::string(12, 'm'),
      std::string(13, 'p'),
      std::string(13, 'q'),
      std::string(64, 'X'),
      std::string("PRFX") + std::string(60, 'A'),
      std::string("PRFX") + std::string(60, 'B'),
      std::string(16 * 1024, 'Z'),
  };
}

} // namespace bytedance::bolt::exec::test
