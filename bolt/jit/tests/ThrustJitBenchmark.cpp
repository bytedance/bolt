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

#ifdef ENABLE_BOLT_JIT

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <llvm/Support/TargetSelect.h>

#include "bolt/jit/ThrustJIT.h"

#include <regex>
#include <thread>
#include <vector>

using namespace bytedance::bolt::jit;

namespace {

const std::string kIrTemplate = R"IR(
    define i64 @function_name(i64 noundef %0, i64 noundef %1)  {
    %3 = add nsw i64 %1, %0
    ret i64 %3
    }
)IR";

constexpr size_t kNoEvictLimit = 1L << 30; // 1 GB - prevents eviction

ThrustJIT* getJit() {
  return ThrustJIT::getInstance();
}

/// Compile `iters` unique modules sequentially.
void compileModules(ThrustJIT* jit, size_t iters, const std::string& prefix) {
  for (size_t i = 0; i < iters; ++i) {
    std::string fn = prefix + std::to_string(i);
    std::string ir =
        std::regex_replace(kIrTemplate, std::regex("function_name"), fn);
    auto tsm = jit->CreateTSModule(fn);
    tsm.withModuleDo(
        [&](llvm::Module& m) { jit->AddIRIntoModule(ir.c_str(), &m); });
    auto mod = jit->CompileModule(std::move(tsm));
    folly::doNotOptimizeAway(mod);
  }
}

// ---- Sequential compile (no eviction) ------------------------------------

BENCHMARK(SeqCompile, iters) {
  folly::BenchmarkSuspender suspender;
  auto* jit = getJit();
  jit->GetCache().clear();
  jit->SetMemoryLimit(kNoEvictLimit);
  suspender.dismiss();

  compileModules(jit, iters, "seq_");
}

BENCHMARK_DRAW_LINE();

// ---- Sequential compile with eviction ------------------------------------

BENCHMARK(SeqCompileEvict, iters) {
  folly::BenchmarkSuspender suspender;
  auto* jit = getJit();
  jit->GetCache().clear();
  jit->SetMemoryLimit(1024);
  suspender.dismiss();

  compileModules(jit, iters, "evict_");
}

BENCHMARK_DRAW_LINE();

// ---- Concurrent compile --------------------------------------------------

constexpr int kConcThreads = 8;

void concurrentCompile(
    ThrustJIT* jit,
    size_t modulesPerThread,
    const std::string& prefix) {
  std::vector<std::jthread> threads;
  for (int t = 0; t < kConcThreads; ++t) {
    threads.emplace_back([&, t]() {
      for (size_t i = 0; i < modulesPerThread; ++i) {
        std::string fn =
            prefix + "t" + std::to_string(t) + "_" + std::to_string(i);
        std::string ir =
            std::regex_replace(kIrTemplate, std::regex("function_name"), fn);
        auto tsm = jit->CreateTSModule(fn);
        tsm.withModuleDo(
            [&](llvm::Module& m) { jit->AddIRIntoModule(ir.c_str(), &m); });
        auto mod = jit->CompileModule(std::move(tsm));
        folly::doNotOptimizeAway(mod);
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
}

BENCHMARK(ConcCompile, iters) {
  folly::BenchmarkSuspender suspender;
  auto* jit = getJit();
  jit->GetCache().clear();
  jit->SetMemoryLimit(kNoEvictLimit);
  suspender.dismiss();

  concurrentCompile(jit, iters, "conc_");
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}

#else // !ENABLE_BOLT_JIT

int main() {
  return 0;
}

#endif // ENABLE_BOLT_JIT
