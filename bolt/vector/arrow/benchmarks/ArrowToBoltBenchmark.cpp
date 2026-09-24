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

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <string>

#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorMaker.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::exec::test;

namespace {

static void mockRelease(ArrowSchema*) {}
static void mockReleaseAllocated(ArrowSchema* schema) {
  delete schema;
}

ArrowSchema makeArrowSchema(
    const char* format,
    std::optional<const char*> dictionaryFormat = std::nullopt) {
  ArrowSchema* dictionary = nullptr;

  if (dictionaryFormat.has_value()) {
    dictionary = new ArrowSchema{
        .format = dictionaryFormat.value(),
        .name = nullptr,
        .metadata = nullptr,
        .flags = 0,
        .n_children = 0,
        .children = nullptr,
        .dictionary = nullptr,
        .release = mockReleaseAllocated,
        .private_data = nullptr,
    };
  }

  return ArrowSchema{
      .format = format,
      .name = nullptr,
      .metadata = nullptr,
      .flags = 0,
      .n_children = 0,
      .children = nullptr,
      .dictionary = dictionary,
      .release = mockRelease,
      .private_data = nullptr,
  };
}

TypePtr runImportFromArrow(uint32_t, const char* format) {
  auto arrowSchema = makeArrowSchema(format);
  auto type = importFromArrow(arrowSchema);
  arrowSchema.release(&arrowSchema);
  return type;
}

TypePtr runImportFromArrowComplex(
    uint32_t,
    const char* mainFormat,
    const std::vector<const char*>& childrenFormat,
    const std::vector<const char*>& colNames = {},
    const std::vector<const char*>& dictionaries = {}) {
  std::vector<ArrowSchema> schemas;
  std::vector<ArrowSchema*> schemaPtrs;

  schemas.resize(childrenFormat.size());
  schemaPtrs.resize(childrenFormat.size());

  for (size_t i = 0; i < childrenFormat.size(); ++i) {
    std::optional<const char*> dictionary;

    if (dictionaries.size() > i && dictionaries[i]) {
      dictionary = dictionaries[i];
    }

    schemas[i] = makeArrowSchema(childrenFormat[i], dictionary);

    if (colNames.size() > i) {
      schemas[i].name = colNames[i];
    }
    schemaPtrs[i] = &schemas[i];
  }

  auto mainSchema = makeArrowSchema(mainFormat);
  if (strcmp(mainFormat, "+m") == 0) {
    // Arrow wraps key and value in a struct.
    auto child = makeArrowSchema("+s");
    auto children = &child;
    child.n_children = schemaPtrs.size();
    child.children = schemaPtrs.data();
    mainSchema.n_children = 1;
    mainSchema.children = &children;
    return importFromArrow(mainSchema);
  } else {
    mainSchema.n_children = (int64_t)schemaPtrs.size();
    mainSchema.children = schemaPtrs.data();
    return importFromArrow(mainSchema);
  }
}

BENCHMARK_NAMED_PARAM(runImportFromArrow, integer, "i");
BENCHMARK_NAMED_PARAM(runImportFromArrow, bigint, "l");
BENCHMARK_NAMED_PARAM(runImportFromArrow, real, "f");
BENCHMARK_NAMED_PARAM(runImportFromArrow, double_, "g");
BENCHMARK_NAMED_PARAM(runImportFromArrow, varchar, "u");

BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexBigintArray,
    "+l",
    {"l"});
BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexTimestampArray,
    "+l",
    {"tsn:"});
BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexDateArray,
    "+l",
    {"tdD"});
BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexVarcharArray,
    "+l",
    {"U"});

BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexDecimalArray,
    "+l",
    {"d:10,5"});
BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexDecimalArrayOne,
    "+l",
    {"d:20,14"});

BENCHMARK_NAMED_PARAM(runImportFromArrowComplex, complexMap, "+m", {"U", "b"});
BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexMapOne,
    "+m",
    {"s", "f"});

BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexRow,
    "+s",
    {"s", "f", "u", "b"});

BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexDictionary,
    "+s",
    {"i", "i", "i"},
    {},
    {"s", "f", "d:38,2"});

BENCHMARK_NAMED_PARAM(
    runImportFromArrowComplex,
    complexNamedDictionary,
    "+s",
    {"i", "i", "i"},
    {"1", "2", "3"},
    {"s", "f", "d:38,2"});

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
