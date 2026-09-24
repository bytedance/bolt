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

/// Exercises the reference encoder and validator with nothing else linked in:
/// no engine, no vector library, not even gtest.
///
/// Its first job is to keep that true. The reference implementation is the
/// oracle the engine's own Writer and Reader are judged against, which only
/// works while it stays independent of them. A comment cannot enforce that
/// and neither can the library target, since the archive would link happily
/// and the other test executable pulls the engine in regardless. This target
/// is where an include of a vector or exec header stops being buildable.
///
/// Its second job is to be the fast check: it runs the whole boundary corpus
/// through every encoding option in a few milliseconds, so the format can be
/// worked on without waiting for the engine to build.

#include <cstdio>

#include "bolt/shuffle/sparksql/tests/columnar_payload/Generator.h"
#include "bolt/shuffle/sparksql/tests/columnar_payload/Validator.h"

namespace {

using namespace bytedance::bolt::shuffle::sparksql::test;

int failures = 0;

void fail(const char* what, const std::string& detail) {
  std::printf("FAIL: %s: %s\n", what, detail.c_str());
  ++failures;
}

/// The option combinations the corpus is run through, matching the ones the
/// gtest suite uses so that a failure here reproduces there.
std::vector<GeneratorOptions> variants() {
  std::vector<GeneratorOptions> all;
  for (int layout = 0; layout < 3; ++layout) {
    for (int compress = 0; compress < 2; ++compress) {
      for (const size_t runCount : {size_t{1}, size_t{3}}) {
        for (int dictionary = 0; dictionary < 2; ++dictionary) {
          for (int policy = 0; policy < 6; ++policy) {
            GeneratorOptions options;
            options.layout = static_cast<CompressionLayout>(layout);
            options.compress = compress != 0;
            options.compressNullBody = options.compress && runCount == 3;
            options.runCount = runCount;
            options.useDictionary = dictionary != 0;
            options.encodingPolicy = static_cast<EncodingPolicy>(policy);
            options.minimalEncodingWidth = (policy % 2) == 0;
            all.push_back(options);
          }
        }
      }
    }
  }
  return all;
}

} // namespace

int main() {
  IdentityCodec identity;
  MaskCodec mask;
  GenerationStats coverage;
  size_t payloads = 0;
  size_t index = 0;

  for (const auto& entry : boundaryCorpus()) {
    for (const auto& options : variants()) {
      Codec* codec = (index++ % 2) != 0 ? static_cast<Codec*>(&identity)
                                        : static_cast<Codec*>(&mask);
      GeneratedPayload generated;
      std::string error;
      ColumnarPayloadGenerator generator(codec, options);
      if (!generator.generate(entry.table, generated, error)) {
        fail(entry.name, "generate: " + error);
        continue;
      }
      coverage.merge(generated.stats);
      ++payloads;

      ValidationOptions validationOptions;
      validationOptions.payloadSizeProvided = true;
      validationOptions.payloadSize = generated.bytes.size();
      ColumnarPayloadValidator validator(
          codec, entry.table.schema(), validationOptions);
      const auto result = validator.validate(generated.bytes);
      if (!result.ok()) {
        fail(entry.name, result.describe());
      } else if (!(result.decoded == entry.table)) {
        fail(entry.name, "round trip mismatch");
      }
    }
  }

  // Coverage is asserted, not assumed: a change that stops emitting one of
  // the encoding paths fails here rather than quietly narrowing the corpus.
  if (const char* gap = coverage.firstGap()) {
    fail("coverage", std::string("never generated: ") + gap);
  }

  std::printf(
      "%zu payloads from %zu corpus tables; %s\n",
      payloads,
      boundaryCorpus().size(),
      failures == 0 ? "all pass" : "FAILURES");
  return failures == 0 ? 0 : 1;
}
