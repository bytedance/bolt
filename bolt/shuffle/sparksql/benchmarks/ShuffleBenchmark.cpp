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

/// The shuffle benchmark suite: a fixed set of cases, each run through the
/// Cell, V1 and V2 writers and their readers on identical input.
///
/// Every case is two folly benchmarks per writer: `<case>/<writer>/write`
/// times one writer lifecycle (split every batch, stop) and
/// `<case>/<writer>/read` times reading its output back; both count rows, so
/// folly reports ns per row. Each also fills folly user counters with the
/// steps behind that time - the writer's split, stop, convert, flatten, pid,
/// combine, compress, write, evict and reclaim times, bytes and windows,
/// dictionary hits, memory peaks; the reader's decompress, decode and merge
/// times - so versions compare step by step. --bm_json_verbose=<file> keeps
/// them all as raw data.
///
/// Cases are frozen: an id never changes meaning, new shapes get new ids.
/// Input is generated from a seed derived from the case id, and every read
/// is checked against the input by row count and an order-independent hash.
///
///   A  data shapes, P=1024, 1 GiB: narrow and random ints, doubles, short,
///      medium and long strings, low-cardinality strings (random and
///      clustered order), a store_sales-like mix, 100 wide columns, complex
///      types, and the production shape (P=30000, dictionary-wrapped input);
///      nested types: arrays of structs, maps of arrays, long arrays, the
///      complex types with nulls, four levels deep
///   B  partition counts 200/4096/30000 on ints, strings, dictionary
///      strings, P=30000 on the complex types with and without the struct;
///      P=1 on the mix
///   C  10% and 50% nulls on the same three; a wide table with sparse
///      nulls, an all-null column, stale all-set null buffers; nulls in
///      long arrays, in arrays of structs, in arrays of their own and of
///      structs themselves, nested fields null throughout, low-cardinality
///      complex values and arrays
///   D  16 and 256 MiB inputs on narrow ints and the wide table: the fixed
///      cost of a writer against its throughput
///   E  one partition taking half the rows, on strings, dictionary strings
///      and the mix
///   F  a 256 MiB task memory limit enforced by the Spark memory manager,
///      so every writer spills through reclaim: ints, medium and long
///      strings, dictionary strings; the complex types, nested nulls, 50%
///      nulls, optional fields
///
/// --bolt_benchmark_shuffle_tier picks the set: smoke (A1, A5, A7, A8 at
/// 64 MiB), standard (A, B, C) or full (A to F). --bm_regex narrows it.
/// Compare versions by running each build's binary alternately on one
/// pinned core, two rounds or more, and comparing the counters.

#include <folly/Benchmark.h>
#include <folly/hash/Hash.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <fstream>
#include <functional>
#include <limits>
#include <random>
#include <regex>

#include <arrow/io/memory.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/sparksql/tests/MemoryTestUtils.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
#include "bolt/shuffle/sparksql/BoltShuffleReader.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleReader.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

DEFINE_string(
    bolt_benchmark_shuffle_tier,
    "standard",
    "Cases to register: smoke, standard or full.");
DEFINE_string(
    bolt_benchmark_shuffle_writers,
    "Cell,V1,V2",
    "Writers to run, of Cell, V1, V2; whenever Cell runs, CellNoDict joins "
    "the dictionary cases and CellUnsealedNulls, which never seals a window "
    "for its null memory, the cases with heavy nulls.");
DEFINE_int64(
    bolt_benchmark_shuffle_input_mb,
    0,
    "Input per case in MiB, overriding the case's own; 0 keeps it.");
DEFINE_string(
    bolt_benchmark_shuffle_dir,
    "/tmp",
    "Directory of the data and spill files.");
DEFINE_bool(
    bolt_benchmark_shuffle_verify,
    true,
    "Check each writer's output against the input once, untimed.");
DEFINE_bool(
    bolt_benchmark_shuffle_list,
    false,
    "Print the cases of the tier and exit.");
DEFINE_string(
    bolt_benchmark_shuffle_codec,
    "lz4",
    "Shuffle compression of every writer and reader: lz4 (LZ4 frame) or "
    "zstd.");
DEFINE_int32(
    bolt_benchmark_shuffle_codec_level,
    0,
    "Compression level; 0 keeps the codec's default.");
DEFINE_bool(
    bolt_benchmark_shuffle_cell_stream_compression,
    true,
    "Cell writers compress runs through the codec's stream form; false "
    "compresses each frame at once from a gathered workspace.");
DEFINE_bool(
    bolt_benchmark_shuffle_cell_adaptive_compression,
    true,
    "Cell writers store, without a codec call, the frames of a kind that "
    "last did not shrink, trying one in ten.");

using namespace bytedance::bolt;
using namespace bytedance::bolt::shuffle::sparksql;

namespace {

arrow::Compression::type benchmarkCodec() {
  if (FLAGS_bolt_benchmark_shuffle_codec == "lz4") {
    return arrow::Compression::LZ4_FRAME;
  }
  if (FLAGS_bolt_benchmark_shuffle_codec == "zstd") {
    return arrow::Compression::ZSTD;
  }
  BOLT_FAIL(
      "Unknown --bolt_benchmark_shuffle_codec: {}",
      FLAGS_bolt_benchmark_shuffle_codec);
}

int32_t benchmarkCodecLevel() {
  return FLAGS_bolt_benchmark_shuffle_codec_level == 0
      ? kDefaultCompressionLevel
      : FLAGS_bolt_benchmark_shuffle_codec_level;
}

constexpr int32_t kRowsPerBatch = 4096;
constexpr int64_t kSplitMemLimit = 1LL << 30;

enum Tier : uint32_t { kSmoke = 1, kStandard = 2, kFull = 4 };

enum class Family {
  kIntNarrow,
  kIntRandom,
  kDouble,
  kStrShort,
  kStrMedium,
  kStrLong,
  kStrLowCard,
  kStrLowCardClustered,
  kMixed,
  kWide,
  kComplex,
  kProd,
  kWideSparse,
  kDeadColumn,
  kStaleNulls,
  kStrRealistic,
  kQ22Rollup,
  kDeadWide,
  kWideOptional,
  kArrayStruct,
  kMapStringArray,
  kArrayDouble,
  kArrayBigint,
  kDeepNested,
  kArrayIntNulls,
  kNestedOptional,
  kNestedDense,
  kNestedDead,
  kStructDense,
  kStructChain,
  kLowCardComplex,
  kContainers,
  kArraysOptional,
  kLowCardArray,
};

const char* familyName(Family family) {
  switch (family) {
    case Family::kIntNarrow:
      return "int_narrow";
    case Family::kIntRandom:
      return "int_random";
    case Family::kDouble:
      return "double";
    case Family::kStrShort:
      return "str_short";
    case Family::kStrMedium:
      return "str_medium";
    case Family::kStrLong:
      return "str_long";
    case Family::kStrLowCard:
      return "str_lowcard";
    case Family::kStrLowCardClustered:
      return "str_lowcard_clustered";
    case Family::kMixed:
      return "mixed";
    case Family::kWide:
      return "wide100";
    case Family::kComplex:
      return "complex";
    case Family::kProd:
      return "prod";
    case Family::kWideSparse:
      return "wide100_sparse_nulls";
    case Family::kDeadColumn:
      return "int_narrow_dead_column";
    case Family::kStaleNulls:
      return "int_narrow_stale_nulls";
    case Family::kStrRealistic:
      return "str_realistic";
    case Family::kQ22Rollup:
      return "q22_rollup";
    case Family::kDeadWide:
      return "int_narrow_dead_wide";
    case Family::kWideOptional:
      return "wide50_optional";
    case Family::kArrayStruct:
      return "array_struct";
    case Family::kMapStringArray:
      return "map_str_array";
    case Family::kArrayDouble:
      return "array_double128";
    case Family::kArrayBigint:
      return "array_bigint1k";
    case Family::kDeepNested:
      return "array_map_array_struct";
    case Family::kArrayIntNulls:
      return "array_int1k";
    case Family::kNestedOptional:
      return "array_struct50_optional";
    case Family::kNestedDense:
      return "array_struct20";
    case Family::kNestedDead:
      return "array_struct_dead32";
    case Family::kStructDense:
      return "struct50";
    case Family::kStructChain:
      return "struct_chain8";
    case Family::kLowCardComplex:
      return "lowcard_complex";
    case Family::kContainers:
      return "array_map";
    case Family::kArraysOptional:
      return "arrays50_optional";
    case Family::kLowCardArray:
      return "lowcard_array";
  }
  return "?";
}

bool isDictionaryFamily(Family family) {
  return family == Family::kStrLowCard ||
      family == Family::kStrLowCardClustered || family == Family::kProd ||
      family == Family::kQ22Rollup || family == Family::kLowCardComplex ||
      family == Family::kLowCardArray;
}

/// Families with ARRAY, MAP or ROW columns.
bool isNestedFamily(Family family) {
  switch (family) {
    case Family::kComplex:
    case Family::kArrayStruct:
    case Family::kMapStringArray:
    case Family::kArrayDouble:
    case Family::kArrayBigint:
    case Family::kDeepNested:
    case Family::kArrayIntNulls:
    case Family::kNestedOptional:
    case Family::kNestedDense:
    case Family::kNestedDead:
    case Family::kStructDense:
    case Family::kStructChain:
    case Family::kLowCardComplex:
    case Family::kContainers:
    case Family::kArraysOptional:
    case Family::kLowCardArray:
      return true;
    default:
      return false;
  }
}

struct CaseSpec {
  std::string id;
  Family family;
  int32_t partitions;
  int32_t nullPercent;
  int64_t inputMb;
  bool skewed;
  /// Task memory limit in MiB; nonzero runs the writer as a task operator
  /// under the Spark memory manager.
  int64_t memLimitMb;
  uint32_t tiers;
  /// The null rate of the families that take one in per mille rather than
  /// nullPercent.
  int32_t nullPerMille{0};

  std::string name() const {
    return id + "." + familyName(family);
  }
};

/// The cases whose nulls can fill the null state, which CellUnsealedNulls
/// joins: the nested ones with nulls at some level, and the flat ones with
/// nulls in half the rows or more of several columns.
bool hasHeavyNulls(const CaseSpec& spec) {
  if (isNestedFamily(spec.family)) {
    return spec.nullPercent > 0 || spec.nullPerMille > 0 ||
        spec.family == Family::kNestedOptional ||
        spec.family == Family::kArraysOptional;
  }
  return (spec.family == Family::kIntNarrow && spec.nullPercent >= 50) ||
      spec.family == Family::kWideOptional;
}

/// The frozen case list. Append only: an id keeps its meaning across
/// versions so results stay comparable.
std::vector<CaseSpec> caseSpecs() {
  constexpr uint32_t kAll = kSmoke | kStandard | kFull;
  constexpr uint32_t kStd = kStandard | kFull;
  std::vector<CaseSpec> c;
  const auto add = [&](std::string id,
                       Family family,
                       int32_t partitions,
                       uint32_t tiers,
                       int32_t nullPercent = 0,
                       int64_t inputMb = 1024,
                       bool skewed = false,
                       int64_t memLimitMb = 0) {
    c.push_back(
        {std::move(id),
         family,
         partitions,
         nullPercent,
         inputMb,
         skewed,
         memLimitMb,
         tiers});
  };
  // A: data shapes.
  add("A1", Family::kIntNarrow, 1024, kAll);
  add("A2", Family::kIntRandom, 1024, kStd);
  add("A3", Family::kDouble, 1024, kStd);
  add("A4", Family::kStrShort, 1024, kStd);
  add("A5", Family::kStrMedium, 1024, kAll);
  add("A6", Family::kStrLong, 1024, kStd);
  add("A7", Family::kStrLowCard, 1024, kAll);
  add("A7c", Family::kStrLowCardClustered, 1024, kStd);
  add("A8", Family::kMixed, 1024, kAll);
  add("A9", Family::kWide, 1024, kStd);
  add("A10", Family::kComplex, 1024, kStd);
  add("A11", Family::kProd, 30000, kStd);
  // B: partition counts.
  for (const auto& [prefix, family] :
       {std::pair{"B1", Family::kIntNarrow},
        std::pair{"B5", Family::kStrMedium},
        std::pair{"B7", Family::kStrLowCard}}) {
    for (const int32_t partitions : {200, 4096, 30000}) {
      add(std::string(prefix) + "-P" + std::to_string(partitions),
          family,
          partitions,
          kStd);
    }
  }
  add("B8-P1", Family::kMixed, 1, kStd);
  // C: nulls.
  for (const auto& [prefix, family] :
       {std::pair{"C1", Family::kIntNarrow},
        std::pair{"C5", Family::kStrMedium},
        std::pair{"C7", Family::kStrLowCard}}) {
    for (const int32_t nulls : {10, 50}) {
      add(std::string(prefix) + "-N" + std::to_string(nulls),
          family,
          1024,
          kStd,
          nulls);
    }
  }
  add("C-wide-sparse", Family::kWideSparse, 200, kStd);
  add("C-dead-column", Family::kDeadColumn, 1024, kStd);
  add("C-stale-nulls", Family::kStaleNulls, 1024, kStd);
  // D: input size.
  for (const auto& [prefix, family] :
       {std::pair{"D1", Family::kIntNarrow}, std::pair{"D9", Family::kWide}}) {
    for (const int64_t mb : {16, 256}) {
      add(std::string(prefix) + "-" + std::to_string(mb) + "MB",
          family,
          1024,
          kFull,
          0,
          mb);
    }
  }
  // E: skew.
  add("E5", Family::kStrMedium, 1024, kFull, 0, 1024, true);
  add("E7", Family::kStrLowCard, 1024, kFull, 0, 1024, true);
  add("E8", Family::kMixed, 1024, kFull, 0, 1024, true);
  // F: memory limited.
  add("F1", Family::kIntNarrow, 1024, kFull, 0, 1024, false, 256);
  add("F5", Family::kStrMedium, 1024, kFull, 0, 1024, false, 256);
  add("F6", Family::kStrLong, 1024, kFull, 0, 1024, false, 256);
  add("F7", Family::kStrLowCard, 1024, kFull, 0, 1024, false, 256);
  // Compressible data: strings with real structure, and TPC-DS q22's rollup
  // shuffle. Spark's default 200 partitions for q22.
  add("A12", Family::kStrRealistic, 1024, kStd);
  add("A13", Family::kQ22Rollup, 200, kStd);
  add("F8", Family::kStrRealistic, 1024, kFull, 0, 1024, false, 256);
  add("F9", Family::kQ22Rollup, 200, kFull, 0, 1024, false, 256);
  // Many columns null throughout, as a wide table's unused ones.
  add("C-dead-wide", Family::kDeadWide, 1024, kStd);
  add("C-dead-wide-P30000", Family::kDeadWide, 30000, kStd);
  // Optional fields of a wide table: nulls rare, or values rare.
  add("C-wide-optional", Family::kWideOptional, 1024, kStd);
  // Nested types: arrays of structs, maps of arrays, long arrays, the
  // complex types with nulls at every level, four levels deep; the complex
  // types at P=30000 and under the memory limit.
  add("A14", Family::kArrayStruct, 1024, kStd);
  add("A15", Family::kMapStringArray, 1024, kStd);
  add("A16", Family::kArrayDouble, 1024, kStd);
  add("A17", Family::kArrayBigint, 1024, kStd);
  add("A18", Family::kComplex, 1024, kStd, 10);
  add("A19", Family::kDeepNested, 1024, kStd, 5);
  add("B-nested-P30000", Family::kComplex, 30000, kStd);
  add("F10", Family::kComplex, 1024, kFull, 0, 1024, false, 256);
  // Nulls among the elements of long arrays, in per mille.
  for (const auto& [suffix, perMille] :
       {std::pair{"N0", 0},
        std::pair{"N0p1", 1},
        std::pair{"N1", 10},
        std::pair{"N50", 500}}) {
    add(std::string("C-array-nulls-") + suffix,
        Family::kArrayIntNulls,
        1024,
        kStd);
    c.back().nullPerMille = perMille;
  }
  for (const auto& [suffix, perMille] :
       {std::pair{"N1", 10}, std::pair{"N50", 500}}) {
    add(std::string("F-array-nulls-") + suffix,
        Family::kArrayIntNulls,
        1024,
        kFull,
        0,
        1024,
        false,
        256);
    c.back().nullPerMille = perMille;
  }
  // The optional fields of a wide table in an array of structs, the fields
  // of dense structs with nulls, and structs of fields null throughout.
  add("C-nested-optional", Family::kNestedOptional, 1024, kStd);
  add("C-nested-optional-P30000", Family::kNestedOptional, 30000, kStd);
  add("F-nested-optional-P30000",
      Family::kNestedOptional,
      30000,
      kFull,
      0,
      1024,
      false,
      256);
  add("C-nested-dense", Family::kNestedDense, 1024, kStd, 10);
  add("C-nested-dense-P30000", Family::kNestedDense, 30000, kStd, 10);
  add("F-nested-dense",
      Family::kNestedDense,
      1024,
      kFull,
      10,
      1024,
      false,
      256);
  add("F-nested-dense-P30000",
      Family::kNestedDense,
      30000,
      kFull,
      10,
      1024,
      false,
      256);
  add("C-nested-dead", Family::kNestedDead, 1024, kStd);
  add("C-nested-dead-P30000", Family::kNestedDead, 30000, kStd);
  // The null-heavy flat shapes under the memory limit.
  add("F-C1-N50", Family::kIntNarrow, 1024, kFull, 50, 1024, false, 256);
  add("F-wide-optional",
      Family::kWideOptional,
      1024,
      kFull,
      0,
      1024,
      false,
      256);
  // Null structs: in an array of wide ones, a wide one alone, eight nested
  // in one another. Then complex values of four values each.
  add("C-nested-optional-S10", Family::kNestedOptional, 1024, kStd, 10);
  add("C-struct-dense-S10", Family::kStructDense, 1024, kStd, 10);
  add("C-struct-chain", Family::kStructChain, 1024, kStd, 10);
  add("C-lowcard-complex", Family::kLowCardComplex, 1024, kStd);
  // The P=30000 nested shapes without structs: the complex types but the
  // struct, and the optional fields as arrays of their own.
  add("B-containers-P30000", Family::kContainers, 30000, kStd);
  add("C-arrays-optional-P30000", Family::kArraysOptional, 30000, kStd);
  add("F-arrays-optional-P30000",
      Family::kArraysOptional,
      30000,
      kFull,
      0,
      1024,
      false,
      256);
  // Arrays of four values few and small enough for a string dictionary.
  add("C-lowcard-array", Family::kLowCardArray, 1024, kStd);
  return c;
}

uint64_t clockNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

/// The input's pool; writers and readers get a fresh leaf each, so their
/// peaks are their own.
memory::MemoryPool* inputPool() {
  static std::shared_ptr<memory::MemoryPool> pool =
      memory::memoryManager()->addLeafPool("shuffle_benchmark_input");
  return pool.get();
}

/// A leaf of a root pool of its own: the default root does not track
/// usage, and the peak must be this lifecycle's alone. The root has no
/// capacity limit, as a task's pool by default.
std::shared_ptr<memory::MemoryPool> freshPool(const std::string& name) {
  static int64_t sequence = 0;
  auto root = memory::memoryManager()->addRootPool(
      name + "_" + std::to_string(sequence++));
  auto leaf = root->addLeafChild("leaf");
  // The leaf keeps its parent alive.
  return leaf;
}

// ---- Process memory --------------------------------------------------------

int64_t statusKb(const char* key) {
  std::ifstream in("/proc/self/status");
  std::string line;
  const std::string prefix = std::string(key) + ":";
  while (std::getline(in, line)) {
    if (line.rfind(prefix, 0) == 0) {
      return std::stoll(line.substr(prefix.size()));
    }
  }
  return -1;
}

/// Resets VmHWM to the current RSS and returns that RSS in bytes; -1 when
/// the kernel refuses.
int64_t resetPeakRss() {
  std::ofstream clear("/proc/self/clear_refs");
  clear << "5";
  clear.close();
  const int64_t rss = statusKb("VmRSS");
  return rss < 0 ? -1 : rss << 10;
}

/// Growth of the process peak RSS over `base` (from resetPeakRss).
int64_t peakRssGrowth(int64_t base) {
  const int64_t hwm = statusKb("VmHWM");
  return base < 0 || hwm < 0 ? -1 : (hwm << 10) - base;
}

// ---- Input generation ------------------------------------------------------

using Rng = std::mt19937_64;

BufferPtr nullsFor(vector_size_t n, int32_t nullPercent, Rng& rng) {
  if (nullPercent == 0) {
    return nullptr;
  }
  auto nulls = AlignedBuffer::allocate<bool>(n, inputPool(), bits::kNotNull);
  auto* raw = nulls->asMutable<uint64_t>();
  for (vector_size_t i = 0; i < n; ++i) {
    if (static_cast<int32_t>(rng() % 100) < nullPercent) {
      bits::setNull(raw, i);
    }
  }
  return nulls;
}

/// Nulls of `n` rows at `perMille` per mille; nullptr at 0.
BufferPtr nullsPerMille(vector_size_t n, int32_t perMille, Rng& rng) {
  if (perMille == 0) {
    return nullptr;
  }
  auto nulls = AlignedBuffer::allocate<bool>(n, inputPool(), bits::kNotNull);
  auto* raw = nulls->asMutable<uint64_t>();
  for (vector_size_t i = 0; i < n; ++i) {
    if (static_cast<int32_t>(rng() % 1000) < perMille) {
      bits::setNull(raw, i);
    }
  }
  return nulls;
}

/// `n` rows of `value`, a batch of them by default.
template <typename T>
VectorPtr flat(
    const TypePtr& type,
    int32_t nullPercent,
    Rng& rng,
    const std::function<T(vector_size_t)>& value,
    vector_size_t n = kRowsPerBatch) {
  auto values = AlignedBuffer::allocate<T>(n, inputPool());
  auto* raw = values->template asMutable<T>();
  for (vector_size_t i = 0; i < n; ++i) {
    raw[i] = value(i);
  }
  return std::make_shared<FlatVector<T>>(
      inputPool(),
      type,
      nullsFor(n, nullPercent, rng),
      n,
      std::move(values),
      std::vector<BufferPtr>{});
}

template <>
VectorPtr flat<bool>(
    const TypePtr& type,
    int32_t nullPercent,
    Rng& rng,
    const std::function<bool(vector_size_t)>& value,
    vector_size_t n) {
  auto values = AlignedBuffer::allocate<bool>(n, inputPool());
  auto* raw = values->asMutable<uint64_t>();
  for (vector_size_t i = 0; i < n; ++i) {
    bits::setBit(raw, i, value(i));
  }
  return std::make_shared<FlatVector<bool>>(
      inputPool(),
      type,
      nullsFor(n, nullPercent, rng),
      n,
      std::move(values),
      std::vector<BufferPtr>{});
}

/// Strings copied into one buffer; `value` fills `out` for row i.
VectorPtr strings(
    int32_t nullPercent,
    Rng& rng,
    const std::function<void(vector_size_t, std::string& out)>& value,
    const TypePtr& type = VARCHAR(),
    vector_size_t n = kRowsPerBatch) {
  std::vector<std::string> all(n);
  size_t total = 0;
  for (vector_size_t i = 0; i < n; ++i) {
    value(i, all[i]);
    total += all[i].size();
  }
  auto chars = AlignedBuffer::allocate<char>(total, inputPool());
  auto views = AlignedBuffer::allocate<StringView>(n, inputPool());
  auto* rawChars = chars->asMutable<char>();
  auto* rawViews = views->asMutable<StringView>();
  size_t offset = 0;
  for (vector_size_t i = 0; i < n; ++i) {
    ::memcpy(rawChars + offset, all[i].data(), all[i].size());
    rawViews[i] =
        StringView(rawChars + offset, static_cast<int32_t>(all[i].size()));
    offset += all[i].size();
  }
  return std::make_shared<FlatVector<StringView>>(
      inputPool(),
      type,
      nullsFor(n, nullPercent, rng),
      n,
      std::move(views),
      std::vector<BufferPtr>{std::move(chars)});
}

/// The entries of a container column, laid out back to back.
struct Entries {
  BufferPtr offsets;
  BufferPtr sizes;
  /// Elements of all the entries together.
  vector_size_t total{0};
};

Entries entries(const std::vector<vector_size_t>& sizes) {
  const auto n = static_cast<vector_size_t>(sizes.size());
  Entries result;
  result.offsets = allocateOffsets(n, inputPool());
  result.sizes = allocateSizes(n, inputPool());
  auto* rawOffsets = result.offsets->asMutable<vector_size_t>();
  auto* rawSizes = result.sizes->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < n; ++i) {
    rawOffsets[i] = result.total;
    rawSizes[i] = sizes[i];
    result.total += sizes[i];
  }
  return result;
}

/// `n` entries of `low` to `high` elements, uniform.
Entries
entries(vector_size_t n, vector_size_t low, vector_size_t high, Rng& rng) {
  std::vector<vector_size_t> sizes(n, low);
  if (high > low) {
    for (auto& size : sizes) {
      size += static_cast<vector_size_t>(rng() % (high - low + 1));
    }
  }
  return entries(sizes);
}

/// `n` arrays over `elements`. Null entries keep their elements, as the
/// complex family's.
VectorPtr arrayOf(
    vector_size_t n,
    BufferPtr nulls,
    const Entries& at,
    VectorPtr elements) {
  return std::make_shared<ArrayVector>(
      inputPool(),
      ARRAY(elements->type()),
      std::move(nulls),
      n,
      at.offsets,
      at.sizes,
      std::move(elements));
}

VectorPtr mapOf(
    vector_size_t n,
    BufferPtr nulls,
    const Entries& at,
    VectorPtr keys,
    VectorPtr values) {
  return std::make_shared<MapVector>(
      inputPool(),
      MAP(keys->type(), values->type()),
      std::move(nulls),
      n,
      at.offsets,
      at.sizes,
      std::move(keys),
      std::move(values));
}

VectorPtr structOf(
    std::vector<std::string> names,
    std::vector<VectorPtr> fields,
    BufferPtr nulls,
    vector_size_t n) {
  std::vector<TypePtr> types;
  for (const auto& field : fields) {
    types.push_back(field->type());
  }
  return std::make_shared<RowVector>(
      inputPool(),
      ROW(std::move(names), std::move(types)),
      std::move(nulls),
      n,
      std::move(fields));
}

void randomChars(Rng& rng, size_t length, std::string& out) {
  out.resize(length);
  for (auto& c : out) {
    c = static_cast<char>('a' + rng() % 26);
  }
}

const std::vector<std::string>& lowCardVocabulary() {
  static const std::vector<std::string> vocab = {
      "aaaa",
      "bbbb",
      "cccc",
      "dddd",
      "eeee",
      "ffff",
      "gggg",
      "hhhh",
      "iiii",
      "jjjj",
      "kkkk",
      "llll"};
  return vocab;
}

VectorPtr pids(const CaseSpec& spec, Rng& rng) {
  const int32_t p = spec.partitions;
  return flat<int32_t>(INTEGER(), 0, rng, [&](vector_size_t) {
    // Skewed: half the rows go to partition 0, the rest spread evenly.
    if (spec.skewed && rng() % 2 == 0) {
      return int32_t{0};
    }
    return static_cast<int32_t>(rng() % p);
  });
}

RowVectorPtr makeRow(
    std::vector<std::string> names,
    std::vector<VectorPtr> children) {
  std::vector<TypePtr> types;
  for (const auto& child : children) {
    types.push_back(child->type());
  }
  return std::make_shared<RowVector>(
      inputPool(),
      ROW(std::move(names), std::move(types)),
      BufferPtr(nullptr),
      kRowsPerBatch,
      std::move(children));
}

/// Fixed-width ints shaped as in a table: sequential ids, a time band,
/// small codes, random ints.
std::vector<VectorPtr>
intNarrowColumns(int32_t batch, int32_t nulls, Rng& rng, bool staleNulls) {
  std::vector<VectorPtr> columns{
      flat<int64_t>(
          BIGINT(),
          nulls,
          rng,
          [&](vector_size_t i) {
            return 7'000'000'000LL + int64_t{batch} * kRowsPerBatch + i;
          }),
      flat<int64_t>(
          BIGINT(),
          nulls,
          rng,
          [&](vector_size_t) {
            return 1'726'000'000'000LL +
                static_cast<int64_t>(rng() % 3'600'000);
          }),
      flat<int32_t>(
          INTEGER(),
          nulls,
          rng,
          [&](vector_size_t) { return static_cast<int32_t>(rng() % 1000); }),
      flat<int32_t>(INTEGER(), nulls, rng, [&](vector_size_t) {
        return static_cast<int32_t>(rng());
      })};
  if (staleNulls) {
    // All-set null buffers: mayHaveNulls() holds, no row is null.
    for (auto& column : columns) {
      column->setNulls(
          AlignedBuffer::allocate<bool>(kRowsPerBatch, inputPool(), true));
    }
  }
  return columns;
}

RowVectorPtr makeBatch(const CaseSpec& spec, int32_t batch, Rng& rng) {
  const int32_t nulls = spec.nullPercent;
  auto pid = pids(spec, rng);
  const auto key = [&] {
    return flat<int64_t>(BIGINT(), 0, rng, [&](vector_size_t) {
      return static_cast<int64_t>(rng());
    });
  };
  switch (spec.family) {
    case Family::kIntNarrow:
    case Family::kStaleNulls: {
      auto columns = intNarrowColumns(
          batch, nulls, rng, spec.family == Family::kStaleNulls);
      columns.insert(columns.begin(), pid);
      return makeRow({"pid", "id", "ts", "small", "rnd"}, std::move(columns));
    }
    case Family::kDeadColumn: {
      auto columns = intNarrowColumns(batch, nulls, rng, false);
      columns.insert(columns.begin(), pid);
      columns.push_back(
          BaseVector::createNullConstant(BIGINT(), kRowsPerBatch, inputPool()));
      return makeRow(
          {"pid", "id", "ts", "small", "rnd", "dead"}, std::move(columns));
    }
    case Family::kDeadWide: {
      // 32 columns null in every row: constant nulls, and flat vectors
      // whose every row is null.
      auto columns = intNarrowColumns(batch, nulls, rng, false);
      columns.insert(columns.begin(), pid);
      std::vector<std::string> names{"pid", "id", "ts", "small", "rnd"};
      const std::array<TypePtr, 4> types{
          BIGINT(), INTEGER(), DOUBLE(), VARCHAR()};
      for (int col = 0; col < 32; ++col) {
        names.push_back("dead" + std::to_string(col));
        const auto& type = types[(col / 2) % types.size()];
        if (col % 2 == 0) {
          columns.push_back(
              BaseVector::createNullConstant(type, kRowsPerBatch, inputPool()));
          continue;
        }
        switch (type->kind()) {
          case TypeKind::BIGINT:
            columns.push_back(flat<int64_t>(
                type, 100, rng, [](vector_size_t) { return int64_t{0}; }));
            break;
          case TypeKind::INTEGER:
            columns.push_back(flat<int32_t>(
                type, 100, rng, [](vector_size_t) { return int32_t{0}; }));
            break;
          case TypeKind::DOUBLE:
            columns.push_back(flat<double>(
                type, 100, rng, [](vector_size_t) { return 0.0; }));
            break;
          default:
            columns.push_back(
                strings(100, rng, [](vector_size_t, std::string&) {}));
            break;
        }
      }
      return makeRow(std::move(names), std::move(columns));
    }
    case Family::kIntRandom: {
      std::vector<VectorPtr> columns{pid};
      for (int i = 0; i < 4; ++i) {
        columns.push_back(
            flat<int64_t>(BIGINT(), nulls, rng, [&](vector_size_t) {
              return static_cast<int64_t>(rng());
            }));
      }
      return makeRow({"pid", "r0", "r1", "r2", "r3"}, std::move(columns));
    }
    case Family::kDouble: {
      std::uniform_real_distribution<double> unit(0, 1);
      return makeRow(
          {"pid", "price", "rate", "measure", "noise"},
          {pid,
           flat<double>(
               DOUBLE(),
               nulls,
               rng,
               [&](vector_size_t) { return (rng() % 1'000'000) / 100.0; }),
           flat<double>(
               DOUBLE(), nulls, rng, [&](vector_size_t) { return unit(rng); }),
           flat<double>(
               DOUBLE(),
               nulls,
               rng,
               [&](vector_size_t) { return 1e3 * unit(rng) - 500; }),
           flat<double>(DOUBLE(), nulls, rng, [&](vector_size_t) {
             return std::bit_cast<double>(rng() & 0x3fffffffffffffffULL);
           })});
    }
    case Family::kStrShort:
    case Family::kStrMedium:
    case Family::kStrLong: {
      const auto [low, high] = spec.family == Family::kStrShort
          ? std::pair{8, 32}
          : spec.family == Family::kStrMedium ? std::pair{8, 100}
                                              : std::pair{200, 2000};
      return makeRow(
          {"pid", "key", "value"},
          {pid,
           key(),
           strings(nulls, rng, [&](vector_size_t, std::string& out) {
             randomChars(rng, low + rng() % (high - low), out);
           })});
    }
    case Family::kStrLowCard:
    case Family::kStrLowCardClustered: {
      const auto& vocab = lowCardVocabulary();
      const bool clustered = spec.family == Family::kStrLowCardClustered;
      return makeRow(
          {"pid", "key", "value"},
          {pid,
           key(),
           strings(nulls, rng, [&](vector_size_t i, std::string& out) {
             out = clustered ? vocab[(i / 64) % vocab.size()]
                             : vocab[rng() % vocab.size()];
           })});
    }
    case Family::kMixed: {
      // A store_sales row: date and item keys, a customer key with gaps,
      // store and quantity codes, three money columns, the sale date, a
      // promotion name from a small set and a customer email.
      static const std::vector<std::string> promotions = [] {
        std::vector<std::string> names;
        for (int i = 0; i < 30; ++i) {
          names.push_back(
              "promo_" + std::string(1 + i % 9, 'x') + std::to_string(i));
        }
        return names;
      }();
      const auto money = [&](int64_t max) {
        return flat<int64_t>(
            DECIMAL(7, 2), nulls, rng, [&, max](vector_size_t) {
              return static_cast<int64_t>(100 + rng() % max);
            });
      };
      return makeRow(
          {"pid",
           "sold_date_sk",
           "item_sk",
           "customer_sk",
           "store_sk",
           "quantity",
           "wholesale_cost",
           "list_price",
           "net_paid",
           "sold_date",
           "promotion",
           "email"},
          {pid,
           flat<int32_t>(
               INTEGER(),
               nulls,
               rng,
               [&](vector_size_t) {
                 return static_cast<int32_t>(2450816 + rng() % 1823);
               }),
           flat<int64_t>(
               BIGINT(),
               nulls,
               rng,
               [&](vector_size_t) {
                 return static_cast<int64_t>(rng() % 300000);
               }),
           flat<int64_t>(
               BIGINT(),
               nulls,
               rng,
               [&](vector_size_t) {
                 return static_cast<int64_t>(rng() % 12'000'000);
               }),
           flat<int32_t>(
               INTEGER(),
               nulls,
               rng,
               [&](vector_size_t) {
                 return static_cast<int32_t>(rng() % 400);
               }),
           flat<int32_t>(
               INTEGER(),
               nulls,
               rng,
               [&](vector_size_t) {
                 return static_cast<int32_t>(1 + rng() % 100);
               }),
           money(10'000),
           money(20'000),
           money(2'000'000),
           flat<int32_t>(
               DATE(),
               nulls,
               rng,
               [&](vector_size_t) {
                 return static_cast<int32_t>(10957 + rng() % 1823);
               }),
           strings(
               nulls,
               rng,
               [&](vector_size_t, std::string& out) {
                 out = promotions[rng() % promotions.size()];
               }),
           strings(nulls, rng, [&](vector_size_t, std::string& out) {
             randomChars(rng, 15 + rng() % 26, out);
             out += "@example.com";
           })});
    }
    case Family::kWide: {
      // 100 columns cycling through ten types.
      std::vector<std::string> names{"pid"};
      std::vector<VectorPtr> columns{pid};
      for (int col = 0; col < 100; ++col) {
        names.push_back("c" + std::to_string(col));
        switch (col % 10) {
          case 0:
            columns.push_back(
                flat<int64_t>(BIGINT(), nulls, rng, [&](vector_size_t) {
                  return static_cast<int64_t>(rng() % 1'000'000'000);
                }));
            break;
          case 1:
            columns.push_back(
                flat<int32_t>(INTEGER(), nulls, rng, [&](vector_size_t) {
                  return static_cast<int32_t>(rng() % 100'000);
                }));
            break;
          case 2:
            columns.push_back(
                flat<double>(DOUBLE(), nulls, rng, [&](vector_size_t) {
                  return (rng() % 10'000'000) / 1000.0;
                }));
            break;
          case 3:
            columns.push_back(
                strings(nulls, rng, [&](vector_size_t, std::string& out) {
                  randomChars(rng, 8 + rng() % 17, out);
                }));
            break;
          case 4:
            columns.push_back(
                flat<int32_t>(DATE(), nulls, rng, [&](vector_size_t) {
                  return static_cast<int32_t>(18000 + rng() % 2000);
                }));
            break;
          case 5:
            columns.push_back(
                flat<int64_t>(DECIMAL(7, 2), nulls, rng, [&](vector_size_t) {
                  return static_cast<int64_t>(rng() % 1'000'000);
                }));
            break;
          case 6:
            columns.push_back(
                flat<bool>(BOOLEAN(), nulls, rng, [&](vector_size_t) {
                  return rng() % 2 == 0;
                }));
            break;
          case 7:
            // Microsecond precision: what every writer keeps.
            columns.push_back(
                flat<Timestamp>(TIMESTAMP(), nulls, rng, [&](vector_size_t) {
                  return Timestamp(
                      1'700'000'000 + static_cast<int64_t>(rng() % 100'000'000),
                      (rng() % 1'000'000) * 1000);
                }));
            break;
          case 8:
            columns.push_back(
                flat<int128_t>(DECIMAL(20, 2), nulls, rng, [&](vector_size_t) {
                  return static_cast<int128_t>(rng() % 1'000'000'000'000ULL) *
                      1000;
                }));
            break;
          default:
            columns.push_back(
                flat<int16_t>(SMALLINT(), nulls, rng, [&](vector_size_t) {
                  return static_cast<int16_t>(rng() % 30000);
                }));
            break;
        }
      }
      return makeRow(std::move(names), std::move(columns));
    }
    case Family::kStrRealistic: {
      // Web log fields: a URL over a few hosts and paths with an item id
      // and query tags, and a product title from a word list with a size
      // and a color. Fragments repeat, whole values rarely do: LZ4 takes it
      // to about a third, as it does real text columns, and the values are
      // far too many for a dictionary.
      static const std::vector<std::string> hosts = {
          "www.shop.example.com",
          "m.shop.example.com",
          "api.shop.example.com",
          "img.cdn.example.net",
          "search.example.org",
          "blog.example.org",
          "partners.example.com",
          "checkout.example.com"};
      static const std::vector<std::string> paths = {
          "catalog",
          "electronics",
          "home",
          "garden",
          "kitchen",
          "outdoor",
          "sports",
          "toys",
          "books",
          "music",
          "fashion",
          "beauty"};
      static const std::vector<std::string> sources = {
          "google", "bing", "newsletter", "facebook", "direct", "affiliate"};
      static const std::vector<std::string> words = {
          "wireless",   "portable", "stainless", "steel",  "classic", "premium",
          "compact",    "deluxe",   "organic",   "cotton", "leather", "bamboo",
          "ceramic",    "digital",  "smart",     "ultra",  "slim",    "heavy",
          "duty",       "kids",     "outdoor",   "indoor", "travel",  "kitchen",
          "garden",     "office",   "chair",     "table",  "lamp",    "speaker",
          "headphones", "backpack", "bottle",    "mug",    "jacket",  "shoes",
          "watch",      "charger",  "cable",     "case",   "cover",   "set",
          "pack",       "edition",  "series",    "pro",    "mini",    "max"};
      static const std::vector<std::string> sizes = {"S", "M", "L", "XL"};
      static const std::vector<std::string> colors = {
          "Black", "White", "Navy", "Red", "Green", "Grey", "Beige"};
      // Skewed toward the first entries, as real vocabularies are.
      const auto pick =
          [&](const std::vector<std::string>& from) -> const std::string& {
        return from[std::min(rng() % from.size(), rng() % from.size())];
      };
      return makeRow(
          {"pid", "key", "url", "title"},
          {pid,
           key(),
           strings(
               nulls,
               rng,
               [&](vector_size_t, std::string& out) {
                 out = "https://" + pick(hosts) + "/" + pick(paths) + "/" +
                     pick(paths) + "/item/" +
                     std::to_string(rng() % 2'000'000) +
                     "?ref=" + pick(sources) + "&utm_campaign=spring_" +
                     std::to_string(rng() % 40);
               }),
           strings(nulls, rng, [&](vector_size_t, std::string& out) {
             out.clear();
             const int numWords = 3 + rng() % 5;
             for (int w = 0; w < numWords; ++w) {
               if (w > 0) {
                 out += ' ';
               }
               out += pick(words);
             }
             out += ", " + pick(sizes) + ", " + pick(colors);
           })});
    }
    case Family::kQ22Rollup: {
      // TPC-DS q22's rollup shuffle when the partial aggregate does not
      // reduce: the expand of (product, brand, class, category) row by row.
      // An inventory row becomes five adjacent rows, one per grouping set,
      // the keys a set drops left null; the inventory is scanned by item,
      // so a product spans long runs. Rows go to partitions by their
      // grouping keys. Narrow and repetitive within a partition: it
      // compresses tens of times.
      static const std::vector<std::string> categories = {
          "Books",
          "Children",
          "Electronics",
          "Home",
          "Jewelry",
          "Men",
          "Music",
          "Shoes",
          "Sports",
          "Women"};
      constexpr int64_t kSets = 5;
      constexpr int64_t kInventoryPerProduct = 256;
      std::vector<int32_t> level(kRowsPerBatch);
      std::vector<int32_t> product(kRowsPerBatch);
      std::vector<int64_t> quantity(kRowsPerBatch);
      int64_t onHand = 0;
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        const int64_t row = int64_t{batch} * kRowsPerBatch + i;
        const int64_t inventory = row / kSets;
        level[i] = static_cast<int32_t>(row % kSets);
        product[i] =
            static_cast<int32_t>((inventory / kInventoryPerProduct) % 20'000);
        if (level[i] == 0) {
          onHand = static_cast<int64_t>(rng() % 1000);
        }
        quantity[i] = onHand;
      }
      // Keys from the most specific: a set of level k drops the k first.
      const auto keyColumn = [&](int32_t depth, auto&& value) {
        auto column = strings(0, rng, [&](vector_size_t i, std::string& out) {
          out = level[i] > depth ? std::string() : value(product[i]);
        });
        for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
          if (level[i] > depth) {
            column->setNull(i, true);
          }
        }
        return column;
      };
      // The partition of a row's group: its level and the keys it keeps.
      auto groupPid = flat<int32_t>(INTEGER(), 0, rng, [&](vector_size_t i) {
        // The most specific key the set keeps: product, brand, class,
        // category, or none for the grand total.
        static constexpr int32_t kModulus[] = {20'000, 1000, 100, 10, 1};
        const int32_t group = product[i] % kModulus[level[i]];
        return static_cast<int32_t>(
            folly::hash::hash_combine(level[i], group) % spec.partitions);
      });
      return makeRow(
          {"pid",
           "i_product_name",
           "i_brand",
           "i_class",
           "i_category",
           "spark_grouping_id",
           "sum_qty",
           "count_qty"},
          {std::move(groupPid),
           keyColumn(
               0, [](int32_t p) { return "product" + std::to_string(p); }),
           keyColumn(
               1,
               [](int32_t p) { return "brand#" + std::to_string(p % 1000); }),
           keyColumn(
               2, [](int32_t p) { return "class" + std::to_string(p % 100); }),
           keyColumn(3, [&](int32_t p) { return categories[p % 10]; }),
           flat<int64_t>(
               BIGINT(),
               0,
               rng,
               [&](vector_size_t i) {
                 return static_cast<int64_t>((1 << level[i]) - 1);
               }),
           flat<int64_t>(
               BIGINT(), 0, rng, [&](vector_size_t i) { return quantity[i]; }),
           flat<int64_t>(
               BIGINT(), 0, rng, [&](vector_size_t) { return int64_t{1}; })});
    }
    case Family::kWideOptional: {
      // 50 bigints, half null in 0.1% of rows and half in 99.9%.
      std::vector<std::string> names{"pid"};
      std::vector<VectorPtr> columns{pid};
      for (int col = 0; col < 50; ++col) {
        names.push_back("o" + std::to_string(col));
        auto column =
            flat<int64_t>(BIGINT(), 0, rng, [&, col](vector_size_t i) {
              return int64_t{batch} * kRowsPerBatch + i + col;
            });
        const uint32_t nullPerMille = col < 25 ? 1 : 999;
        auto nulls = AlignedBuffer::allocate<bool>(
            kRowsPerBatch, inputPool(), bits::kNotNull);
        auto* raw = nulls->asMutable<uint64_t>();
        for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
          if (rng() % 1000 < nullPerMille) {
            bits::setNull(raw, i);
          }
        }
        column->setNulls(std::move(nulls));
        columns.push_back(std::move(column));
      }
      return makeRow(std::move(names), std::move(columns));
    }
    case Family::kWideSparse: {
      // 100 bigints, the first two with 1% sparse nulls.
      std::vector<std::string> names{"pid"};
      std::vector<VectorPtr> columns{pid};
      for (int col = 0; col < 100; ++col) {
        names.push_back("c" + std::to_string(col));
        columns.push_back(flat<int64_t>(
            BIGINT(), col < 2 ? 1 : 0, rng, [&, col](vector_size_t i) {
              return int64_t{batch} * kRowsPerBatch + i + col;
            }));
      }
      return makeRow(std::move(names), std::move(columns));
    }
    case Family::kComplex:
    case Family::kContainers: {
      // An array of bigints, a map of int to bigint, a struct of an int and
      // a string; nulls in the containers, the struct and every value but
      // the map keys. The containers family stops before the struct.
      std::vector<vector_size_t> arraySizes(kRowsPerBatch);
      std::vector<vector_size_t> mapSizes(kRowsPerBatch);
      vector_size_t arrayTotal = 0;
      vector_size_t mapTotal = 0;
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        arraySizes[i] = rng() % 9;
        mapSizes[i] = rng() % 5;
        arrayTotal += arraySizes[i];
        mapTotal += mapSizes[i];
      }
      const auto offsetsAndSizes = [&](const std::vector<vector_size_t>& sizes,
                                       BufferPtr& offsets,
                                       BufferPtr& lengths) {
        offsets = allocateOffsets(kRowsPerBatch, inputPool());
        lengths = allocateSizes(kRowsPerBatch, inputPool());
        auto* rawOffsets = offsets->asMutable<vector_size_t>();
        auto* rawSizes = lengths->asMutable<vector_size_t>();
        vector_size_t at = 0;
        for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
          rawOffsets[i] = at;
          rawSizes[i] = sizes[i];
          at += sizes[i];
        }
      };
      // The values, then their nulls; nullptr at no nulls, which draws
      // nothing from the generator.
      const auto elements =
          [&](vector_size_t n, auto value, TypePtr type, int32_t nullPercent) {
            using T = decltype(value(0));
            auto values = AlignedBuffer::allocate<T>(n, inputPool());
            auto* raw = values->template asMutable<T>();
            for (vector_size_t i = 0; i < n; ++i) {
              raw[i] = value(i);
            }
            return std::make_shared<FlatVector<T>>(
                inputPool(),
                std::move(type),
                nullsFor(n, nullPercent, rng),
                n,
                std::move(values),
                std::vector<BufferPtr>{});
          };
      BufferPtr arrayOffsets, arrayLengths, mapOffsets, mapLengths;
      offsetsAndSizes(arraySizes, arrayOffsets, arrayLengths);
      offsetsAndSizes(mapSizes, mapOffsets, mapLengths);
      // One draw from the generator per statement, so the order of the
      // draws is the order of the code.
      auto arrayNulls = nullsFor(kRowsPerBatch, nulls, rng);
      auto arrayElements = elements(
          arrayTotal,
          [&](vector_size_t) { return static_cast<int64_t>(rng() % 100000); },
          BIGINT(),
          nulls);
      auto array = std::make_shared<ArrayVector>(
          inputPool(),
          ARRAY(BIGINT()),
          std::move(arrayNulls),
          kRowsPerBatch,
          arrayOffsets,
          arrayLengths,
          std::move(arrayElements));
      auto mapNulls = nullsFor(kRowsPerBatch, nulls, rng);
      auto mapKeys = elements(
          mapTotal,
          [&](vector_size_t i) { return static_cast<int32_t>(i % 5); },
          INTEGER(),
          0);
      auto mapValues = elements(
          mapTotal,
          [&](vector_size_t) { return static_cast<int64_t>(rng()); },
          BIGINT(),
          nulls);
      auto map = std::make_shared<MapVector>(
          inputPool(),
          MAP(INTEGER(), BIGINT()),
          std::move(mapNulls),
          kRowsPerBatch,
          mapOffsets,
          mapLengths,
          std::move(mapKeys),
          std::move(mapValues));
      if (spec.family == Family::kContainers) {
        return makeRow({"pid", "key", "arr", "map"}, {pid, key(), array, map});
      }
      auto structNulls = nullsFor(kRowsPerBatch, nulls, rng);
      auto structure = std::make_shared<RowVector>(
          inputPool(),
          ROW({"a", "b"}, {INTEGER(), VARCHAR()}),
          std::move(structNulls),
          kRowsPerBatch,
          std::vector<VectorPtr>{
              flat<int32_t>(
                  INTEGER(),
                  nulls,
                  rng,
                  [&](vector_size_t) {
                    return static_cast<int32_t>(rng() % 1000);
                  }),
              strings(nulls, rng, [&](vector_size_t, std::string& out) {
                randomChars(rng, 4 + rng() % 9, out);
              })});
      return makeRow(
          {"pid", "key", "arr", "map", "st"},
          {pid, key(), array, map, structure});
    }
    case Family::kProd: {
      // Dictionary-wrapped input (the scan-side norm, forcing the indexed
      // row loop), a mixed-length vocabulary and 10% nulls at P=30000.
      static const std::vector<std::string> vocab = {
          "us",
          "euro",
          "yuan",
          "franc",
          "krona",
          "peso",
          "real",
          "rupee",
          "won",
          "zloty",
          "baht",
          "lira"};
      auto indices =
          AlignedBuffer::allocate<vector_size_t>(kRowsPerBatch, inputPool());
      auto* raw = indices->asMutable<vector_size_t>();
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        raw[i] = i;
      }
      std::shuffle(raw, raw + kRowsPerBatch, rng);
      const auto wrap = [&](VectorPtr base) {
        return BaseVector::wrapInDictionary(
            nullptr, indices, kRowsPerBatch, std::move(base));
      };
      return makeRow(
          {"pid", "key", "currency"},
          {pid,
           wrap(key()),
           wrap(strings(10, rng, [&](vector_size_t, std::string& out) {
             out = vocab[rng() % vocab.size()];
           }))});
    }
    // The nested families draw from the generator once per statement, so
    // the order of the draws is the order of the code.
    case Family::kArrayStruct: {
      // An array of 0 to 8 structs of an int and a short string.
      const auto at = entries(kRowsPerBatch, 0, 8, rng);
      auto a = flat<int32_t>(
          INTEGER(),
          0,
          rng,
          [&](vector_size_t) { return static_cast<int32_t>(rng() % 1000); },
          at.total);
      auto b = strings(
          0,
          rng,
          [&](vector_size_t, std::string& out) {
            randomChars(rng, 4 + rng() % 9, out);
          },
          VARCHAR(),
          at.total);
      auto elements = structOf({"a", "b"}, {a, b}, nullptr, at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, elements);
      return makeRow({"pid", "key", "arr"}, {pid, key(), array});
    }
    case Family::kMapStringArray: {
      // A map of 0 to 4 low-cardinality words to arrays of 0 to 8 bigints.
      const auto& vocab = lowCardVocabulary();
      const auto mapAt = entries(kRowsPerBatch, 0, 4, rng);
      auto keys = strings(
          0,
          rng,
          [&](vector_size_t, std::string& out) {
            out = vocab[rng() % vocab.size()];
          },
          VARCHAR(),
          mapAt.total);
      const auto arrayAt = entries(mapAt.total, 0, 8, rng);
      auto elements = flat<int64_t>(
          BIGINT(),
          0,
          rng,
          [&](vector_size_t) { return static_cast<int64_t>(rng() % 100000); },
          arrayAt.total);
      auto values = arrayOf(mapAt.total, nullptr, arrayAt, elements);
      auto map = mapOf(kRowsPerBatch, nullptr, mapAt, keys, values);
      return makeRow({"pid", "key", "map"}, {pid, key(), map});
    }
    case Family::kArrayDouble: {
      // An array of 128 prices.
      const auto at = entries(kRowsPerBatch, 128, 128, rng);
      auto prices = flat<double>(
          DOUBLE(),
          0,
          rng,
          [&](vector_size_t) { return (rng() % 1'000'000) / 100.0; },
          at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, prices);
      return makeRow({"pid", "key", "prices"}, {pid, key(), array});
    }
    case Family::kArrayBigint: {
      // An array of 900 to 1100 bigints.
      const auto at = entries(kRowsPerBatch, 900, 1100, rng);
      auto values = flat<int64_t>(
          BIGINT(),
          0,
          rng,
          [&](vector_size_t) { return static_cast<int64_t>(rng() % 100000); },
          at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, values);
      return makeRow({"pid", "key", "values"}, {pid, key(), array});
    }
    case Family::kDeepNested: {
      // Four levels: an array of maps of low-cardinality words to arrays
      // of structs of an int and a timestamp, each level 0 to 3 long, with
      // nulls at every level but the map keys.
      const auto& vocab = lowCardVocabulary();
      const auto outerAt = entries(kRowsPerBatch, 0, 3, rng);
      auto outerNulls = nullsFor(kRowsPerBatch, nulls, rng);
      const auto mapAt = entries(outerAt.total, 0, 3, rng);
      auto mapNulls = nullsFor(outerAt.total, nulls, rng);
      auto keys = strings(
          0,
          rng,
          [&](vector_size_t, std::string& out) {
            out = vocab[rng() % vocab.size()];
          },
          VARCHAR(),
          mapAt.total);
      const auto innerAt = entries(mapAt.total, 0, 3, rng);
      auto innerNulls = nullsFor(mapAt.total, nulls, rng);
      auto structNulls = nullsFor(innerAt.total, nulls, rng);
      auto x = flat<int32_t>(
          INTEGER(),
          nulls,
          rng,
          [&](vector_size_t) { return static_cast<int32_t>(rng() % 1000); },
          innerAt.total);
      // Microsecond precision: what every writer keeps.
      auto y = flat<Timestamp>(
          TIMESTAMP(),
          nulls,
          rng,
          [&](vector_size_t) {
            const auto seconds =
                1'726'000'000 + static_cast<int64_t>(rng() % 3'600'000);
            const auto nanos = (rng() % 1'000'000) * 1000;
            return Timestamp(seconds, nanos);
          },
          innerAt.total);
      auto structs =
          structOf({"x", "y"}, {x, y}, std::move(structNulls), innerAt.total);
      auto inner =
          arrayOf(mapAt.total, std::move(innerNulls), innerAt, structs);
      auto maps = mapOf(outerAt.total, std::move(mapNulls), mapAt, keys, inner);
      auto outer = arrayOf(kRowsPerBatch, std::move(outerNulls), outerAt, maps);
      return makeRow({"pid", "key", "deep"}, {pid, key(), outer});
    }
    case Family::kArrayIntNulls: {
      // An array of 900 to 1100 ints, nullPerMille of them null.
      const auto at = entries(kRowsPerBatch, 900, 1100, rng);
      auto values = flat<int32_t>(
          INTEGER(),
          0,
          rng,
          [&](vector_size_t) { return static_cast<int32_t>(rng() % 100000); },
          at.total);
      values->setNulls(nullsPerMille(at.total, spec.nullPerMille, rng));
      auto array = arrayOf(kRowsPerBatch, nullptr, at, values);
      return makeRow({"pid", "key", "values"}, {pid, key(), array});
    }
    case Family::kNestedOptional: {
      // The optional fields of a wide table in an array of 0 to 8 structs:
      // 50 bigints, half null in 0.1% of the structs and half in 99.9%;
      // nullPercent of the structs null.
      const auto at = entries(kRowsPerBatch, 0, 8, rng);
      std::vector<std::string> names;
      std::vector<VectorPtr> fields;
      for (int col = 0; col < 50; ++col) {
        names.push_back("f" + std::to_string(col));
        auto field = flat<int64_t>(
            BIGINT(),
            0,
            rng,
            [&, col](vector_size_t i) {
              return int64_t{batch} * kRowsPerBatch * 8 + i + col;
            },
            at.total);
        field->setNulls(nullsPerMille(at.total, col < 25 ? 1 : 999, rng));
        fields.push_back(std::move(field));
      }
      auto structNulls = nullsFor(at.total, nulls, rng);
      auto structs = structOf(
          std::move(names),
          std::move(fields),
          std::move(structNulls),
          at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, structs);
      return makeRow({"pid", "key", "arr"}, {pid, key(), array});
    }
    case Family::kNestedDense: {
      // An array of 10 structs of 20 bigints, nullPercent of each field
      // null.
      const auto at = entries(kRowsPerBatch, 10, 10, rng);
      std::vector<std::string> names;
      std::vector<VectorPtr> fields;
      for (int col = 0; col < 20; ++col) {
        names.push_back("f" + std::to_string(col));
        fields.push_back(flat<int64_t>(
            BIGINT(),
            nulls,
            rng,
            [&](vector_size_t) { return static_cast<int64_t>(rng() % 100000); },
            at.total));
      }
      auto structs =
          structOf(std::move(names), std::move(fields), nullptr, at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, structs);
      return makeRow({"pid", "key", "arr"}, {pid, key(), array});
    }
    case Family::kNestedDead: {
      // An array of 0 to 8 structs of four fields with values and 32 null
      // in every row: constant nulls, and flat vectors whose every row is
      // null.
      const auto at = entries(kRowsPerBatch, 0, 8, rng);
      std::vector<std::string> names{"a", "b", "c", "d"};
      std::vector<VectorPtr> fields;
      fields.push_back(flat<int32_t>(
          INTEGER(),
          0,
          rng,
          [&](vector_size_t) { return static_cast<int32_t>(rng() % 1000); },
          at.total));
      fields.push_back(flat<int64_t>(
          BIGINT(),
          0,
          rng,
          [&](vector_size_t) { return static_cast<int64_t>(rng() % 100000); },
          at.total));
      fields.push_back(flat<double>(
          DOUBLE(),
          0,
          rng,
          [&](vector_size_t) { return (rng() % 1'000'000) / 100.0; },
          at.total));
      fields.push_back(strings(
          0,
          rng,
          [&](vector_size_t, std::string& out) {
            randomChars(rng, 4 + rng() % 9, out);
          },
          VARCHAR(),
          at.total));
      const std::array<TypePtr, 4> types{
          BIGINT(), INTEGER(), DOUBLE(), VARCHAR()};
      for (int col = 0; col < 32; ++col) {
        names.push_back("dead" + std::to_string(col));
        const auto& type = types[(col / 2) % types.size()];
        if (col % 2 == 0) {
          fields.push_back(
              BaseVector::createNullConstant(type, at.total, inputPool()));
          continue;
        }
        VectorPtr dead;
        switch (type->kind()) {
          case TypeKind::BIGINT:
            dead = flat<int64_t>(
                type,
                0,
                rng,
                [](vector_size_t) { return int64_t{0}; },
                at.total);
            break;
          case TypeKind::INTEGER:
            dead = flat<int32_t>(
                type,
                0,
                rng,
                [](vector_size_t) { return int32_t{0}; },
                at.total);
            break;
          case TypeKind::DOUBLE:
            dead = flat<double>(
                type, 0, rng, [](vector_size_t) { return 0.0; }, at.total);
            break;
          default:
            dead = strings(
                0, rng, [](vector_size_t, std::string&) {}, type, at.total);
            break;
        }
        dead->setNulls(
            AlignedBuffer::allocate<bool>(at.total, inputPool(), bits::kNull));
        fields.push_back(std::move(dead));
      }
      auto structs =
          structOf(std::move(names), std::move(fields), nullptr, at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, structs);
      return makeRow({"pid", "key", "arr"}, {pid, key(), array});
    }
    case Family::kStructDense: {
      // A struct of 50 bigints, nullPercent of the structs null; the
      // fields have no nulls of their own.
      std::vector<std::string> names;
      std::vector<VectorPtr> fields;
      for (int col = 0; col < 50; ++col) {
        names.push_back("f" + std::to_string(col));
        fields.push_back(flat<int64_t>(BIGINT(), 0, rng, [&](vector_size_t) {
          return static_cast<int64_t>(rng() % 100000);
        }));
      }
      auto structNulls = nullsFor(kRowsPerBatch, nulls, rng);
      auto structure = structOf(
          std::move(names),
          std::move(fields),
          std::move(structNulls),
          kRowsPerBatch);
      return makeRow({"pid", "key", "st"}, {pid, key(), structure});
    }
    case Family::kStructChain: {
      // Eight structs nested in one another, each of a bigint and the next
      // one, nullPercent of each null; the bigints have no nulls of their
      // own.
      VectorPtr chain;
      for (int level = 8; level >= 1; --level) {
        std::vector<std::string> names{"v" + std::to_string(level)};
        std::vector<VectorPtr> fields{
            flat<int64_t>(BIGINT(), 0, rng, [&](vector_size_t) {
              return static_cast<int64_t>(rng() % 100000);
            })};
        if (chain != nullptr) {
          names.push_back("s" + std::to_string(level + 1));
          fields.push_back(std::move(chain));
        }
        auto structNulls = nullsFor(kRowsPerBatch, nulls, rng);
        chain = structOf(
            std::move(names),
            std::move(fields),
            std::move(structNulls),
            kRowsPerBatch);
      }
      return makeRow({"pid", "key", "s1"}, {pid, key(), chain});
    }
    case Family::kLowCardComplex: {
      // A struct of an int and a word, and an array of 0 to 3 words, each
      // of four values only: struct k holds 100 + k and word k, array k the
      // first k of three other words.
      const auto& vocab = lowCardVocabulary();
      std::vector<int32_t> structValue(kRowsPerBatch);
      std::vector<vector_size_t> arrayValue(kRowsPerBatch);
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        structValue[i] = static_cast<int32_t>(rng() % 4);
      }
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        arrayValue[i] = static_cast<vector_size_t>(rng() % 4);
      }
      auto a = flat<int32_t>(INTEGER(), 0, rng, [&](vector_size_t i) {
        return 100 + structValue[i];
      });
      auto b = strings(0, rng, [&](vector_size_t i, std::string& out) {
        out = vocab[structValue[i]];
      });
      auto structure = structOf({"a", "b"}, {a, b}, nullptr, kRowsPerBatch);
      const auto at = entries(arrayValue);
      std::vector<int32_t> word(at.total);
      for (vector_size_t i = 0, next = 0; i < kRowsPerBatch; ++i) {
        for (vector_size_t k = 0; k < arrayValue[i]; ++k) {
          word[next++] = 4 + k;
        }
      }
      auto words = strings(
          0,
          rng,
          [&](vector_size_t i, std::string& out) { out = vocab[word[i]]; },
          VARCHAR(),
          at.total);
      auto tags = arrayOf(kRowsPerBatch, nullptr, at, words);
      return makeRow(
          {"pid", "key", "st", "tags"}, {pid, key(), structure, tags});
    }
    case Family::kArraysOptional: {
      // The optional fields of the nested family as arrays of their own:
      // 50 arrays of bigints, all of one length per row, 0 to 8; half null
      // in 0.1% of the elements and half in 99.9%.
      const auto at = entries(kRowsPerBatch, 0, 8, rng);
      std::vector<std::string> names{"pid", "key"};
      std::vector<VectorPtr> columns{pid, key()};
      for (int col = 0; col < 50; ++col) {
        names.push_back("f" + std::to_string(col));
        auto values = flat<int64_t>(
            BIGINT(),
            0,
            rng,
            [&, col](vector_size_t i) {
              return int64_t{batch} * kRowsPerBatch * 8 + i + col;
            },
            at.total);
        values->setNulls(nullsPerMille(at.total, col < 25 ? 1 : 999, rng));
        columns.push_back(arrayOf(kRowsPerBatch, nullptr, at, values));
      }
      return makeRow(std::move(names), std::move(columns));
    }
    case Family::kLowCardArray: {
      // An array of one of four values - none, either or both of two words
      // - whose serialized forms fit one string dictionary together.
      const auto& vocab = lowCardVocabulary();
      std::vector<uint32_t> mask(kRowsPerBatch);
      std::vector<vector_size_t> sizes(kRowsPerBatch);
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        mask[i] = static_cast<uint32_t>(rng() % 4);
        sizes[i] = static_cast<vector_size_t>((mask[i] & 1) + (mask[i] >> 1));
      }
      const auto at = entries(sizes);
      std::vector<uint32_t> word;
      word.reserve(at.total);
      for (const auto m : mask) {
        for (uint32_t w = 0; w < 2; ++w) {
          if ((m >> w) & 1) {
            word.push_back(w);
          }
        }
      }
      auto words = strings(
          0,
          rng,
          [&](vector_size_t i, std::string& out) { out = vocab[word[i]]; },
          VARCHAR(),
          at.total);
      auto tags = arrayOf(kRowsPerBatch, nullptr, at, words);
      return makeRow({"pid", "key", "tags"}, {pid, key(), tags});
    }
  }
  BOLT_UNREACHABLE();
}

// ---- Cases resident in memory ----------------------------------------------

struct Case {
  CaseSpec spec;
  std::vector<RowVectorPtr> batches; // pid column first
  int64_t rows{0};
  int64_t inputBytes{0};
  uint64_t checksum{0};
};

/// Order-independent hash of the rows of `data` (no pid column).
uint64_t rowsHash(const RowVector& data) {
  uint64_t sum = 0;
  for (vector_size_t row = 0; row < data.size(); ++row) {
    sum += data.hashValueAt(row);
  }
  return sum;
}

RowVectorPtr withoutPid(const RowVectorPtr& batch) {
  const auto& row = batch->type()->asRow();
  std::vector<std::string> names(row.names().begin() + 1, row.names().end());
  std::vector<TypePtr> types(row.children().begin() + 1, row.children().end());
  std::vector<VectorPtr> children(
      batch->children().begin() + 1, batch->children().end());
  return std::make_shared<RowVector>(
      inputPool(),
      ROW(std::move(names), std::move(types)),
      BufferPtr(nullptr),
      batch->size(),
      std::move(children));
}

/// main() resets it before returning: static destruction would otherwise
/// free its vectors after their pool.
std::unique_ptr<Case>& residentCase() {
  static std::unique_ptr<Case> current;
  return current;
}

/// The case, built on first use; one is resident at a time.
const Case& getCase(const CaseSpec& spec) {
  auto& current = residentCase();
  if (current != nullptr && current->spec.id == spec.id) {
    return *current;
  }
  current.reset();
  auto built = std::make_unique<Case>();
  built->spec = spec;
  if (FLAGS_bolt_benchmark_shuffle_input_mb > 0) {
    built->spec.inputMb = FLAGS_bolt_benchmark_shuffle_input_mb;
  }
  Rng rng(std::hash<std::string>{}(spec.id) ^ 0x5eedULL);
  const int64_t target = built->spec.inputMb << 20;
  for (int32_t b = 0; built->inputBytes < target; ++b) {
    auto batch = makeBatch(built->spec, b, rng);
    built->inputBytes += batch->estimateFlatSize();
    built->rows += batch->size();
    built->checksum += rowsHash(*withoutPid(batch));
    built->batches.push_back(std::move(batch));
  }
  current = std::move(built);
  return *current;
}

/// Shallow copies of the batches for one lifecycle: the legacy writers
/// flatten their input's children in place.
std::vector<RowVectorPtr> lifecycleBatches(const Case& c) {
  std::vector<RowVectorPtr> batches;
  batches.reserve(c.batches.size());
  for (const auto& batch : c.batches) {
    batches.push_back(std::make_shared<RowVector>(
        inputPool(),
        batch->type(),
        BufferPtr(nullptr),
        batch->size(),
        batch->children()));
  }
  return batches;
}

RowTypePtr dataType(const Case& c) {
  return asRowType(withoutPid(c.batches[0])->type());
}

// ---- Writers ---------------------------------------------------------------

/// CellNoDict is Cell without string dictionaries; CellUnsealedNulls is
/// Cell that never seals a window for its null memory.
enum class Writer { kCell, kCellNoDict, kV1, kV2, kCellUnsealedNulls };

const char* writerName(Writer writer) {
  switch (writer) {
    case Writer::kCell:
      return "Cell";
    case Writer::kCellNoDict:
      return "CellNoDict";
    case Writer::kV1:
      return "V1";
    case Writer::kV2:
      return "V2";
    case Writer::kCellUnsealedNulls:
      return "CellUnsealedNulls";
  }
  return "?";
}

int32_t writerType(Writer writer) {
  switch (writer) {
    case Writer::kCell:
    case Writer::kCellNoDict:
    case Writer::kCellUnsealedNulls:
      return static_cast<int32_t>(ShuffleWriterType::Cell);
    case Writer::kV1:
      return static_cast<int32_t>(ShuffleWriterType::V1);
    case Writer::kV2:
      return static_cast<int32_t>(ShuffleWriterType::V2);
  }
  return 0;
}

bool isCell(Writer writer) {
  return writer == Writer::kCell || writer == Writer::kCellNoDict ||
      writer == Writer::kCellUnsealedNulls;
}

std::string tempPath(const char* name) {
  std::string path = FLAGS_bolt_benchmark_shuffle_dir + "/bolt_shuffle_bench_" +
      name + "_XXXXXX";
  const int fd = ::mkstemp(path.data());
  BOLT_CHECK_GE(
      fd, 0, "cannot create a file in {}", FLAGS_bolt_benchmark_shuffle_dir);
  ::close(fd);
  return path;
}

const std::string& spillDir() {
  static const std::string dir = [] {
    std::string path =
        FLAGS_bolt_benchmark_shuffle_dir + "/bolt_shuffle_bench_spill_XXXXXX";
    BOLT_CHECK_NOT_NULL(::mkdtemp(path.data()));
    return path;
  }();
  return dir;
}

ShuffleWriterOptions
writerOptions(const Case& c, Writer writer, const std::string& dataFile) {
  ShuffleWriterOptions options;
  options.partitioning = Partitioning::kHash;
  options.forceShuffleWriterType = writerType(writer);
  options.partitionWriterOptions.numPartitions = c.spec.partitions;
  options.partitionWriterOptions.dataFile = dataFile;
  options.partitionWriterOptions.configuredDirs = {spillDir()};
  options.partitionWriterOptions.numSubDirs = 1;
  options.partitionWriterOptions.compressionType = benchmarkCodec();
  options.partitionWriterOptions.compressionLevel = benchmarkCodecLevel();
  options.cellOptions.enableStringDictionary = writer != Writer::kCellNoDict;
  if (writer == Writer::kCellUnsealedNulls) {
    // Neither the null memory nor a reclaim's share of it closes a window.
    options.cellOptions.nullMemLimitBytes = std::numeric_limits<int64_t>::max();
  }
  options.cellOptions.streamCompression =
      FLAGS_bolt_benchmark_shuffle_cell_stream_compression;
  options.cellOptions.adaptiveCompression =
      FLAGS_bolt_benchmark_shuffle_cell_adaptive_compression;
  return options;
}

/// One writer lifecycle and what it reports.
struct Written {
  bool ok{false};
  std::string error;
  int64_t rows{0};
  uint64_t splitNs{0};
  uint64_t stopNs{0};
  uint64_t wallNs{0};
  ShuffleWriterMetrics metrics;
  int64_t poolPeak{0};
  int64_t rssGrowth{-1};
  std::string dataFile;
};

/// A writer lifecycle in three steps, so the benchmark times run() alone:
/// prepare() builds the writer (or the task) and settles the machine,
/// finish() collects what it reports and tears it down.
class Lifecycle {
 public:
  Lifecycle(const Case& c, Writer writer) : case_(c), writer_(writer) {}

  void prepare() {
    result_.dataFile = tempPath("data");
    auto options = writerOptions(case_, writer_, result_.dataFile);
    if (case_.spec.memLimitMb > 0) {
      holder_ = memory::sparksql::test::TestMemoryManagerHolder::create(
          case_.spec.memLimitMb << 20, /*withOperatorReclaim=*/true);
      options.taskAttemptId = holder_->taskAttemptId();
      auto values =
          std::make_shared<core::ValuesNode>("values", lifecycleBatches(case_));
      params_.planNode = std::make_shared<SparkShuffleWriterNode>(
          "writer",
          options,
          [this](const ShuffleWriterMetrics& metrics) { reported_ = metrics; },
          values);
      params_.serialExecution = true;
      params_.queryCtx = core::QueryCtx::create(
          nullptr, core::QueryConfig{{}}, {}, nullptr, holder_->rootPool());
    } else {
      batches_ = lifecycleBatches(case_);
      pool_ = freshPool("writer");
      arrowPool_ = std::make_unique<BoltArrowMemoryPool>(pool_.get());
      const auto& first = case_.batches[0];
      try {
        shuffleWriter_ = BoltShuffleWriter::createShuffleWriter(
            options,
            first->type()->size() - 1,
            first->size(),
            first->estimateFlatSize(),
            kSplitMemLimit,
            pool_.get(),
            arrowPool_.get());
      } catch (const std::exception& e) {
        result_.error = e.what();
      }
    }
    // Earlier lifecycles' dirty pages would otherwise be written back
    // during this one.
    ::sync();
    rssBase_ = resetPeakRss();
  }

  /// The timed part: every split and the stop, or the whole task.
  void run() {
    if (!result_.error.empty()) {
      return;
    }
    try {
      if (holder_ != nullptr) {
        const uint64_t start = clockNs();
        auto cursor = exec::test::TaskCursor::create(params_);
        while (cursor->moveNext()) {
        }
        result_.wallNs = clockNs() - start;
        result_.rows = case_.rows;
      } else {
        const uint64_t splitStart = clockNs();
        for (const auto& batch : batches_) {
          const auto status = shuffleWriter_->split(batch, kSplitMemLimit);
          BOLT_CHECK(status.ok(), "{}", status.ToString());
          result_.rows += batch->size();
        }
        const uint64_t stopStart = clockNs();
        const auto status = shuffleWriter_->stop();
        const uint64_t stopEnd = clockNs();
        BOLT_CHECK(status.ok(), "{}", status.ToString());
        result_.splitNs = stopStart - splitStart;
        result_.stopNs = stopEnd - stopStart;
        result_.wallNs = stopEnd - splitStart;
      }
      result_.ok = true;
    } catch (const std::exception& e) {
      result_.error = e.what();
    }
  }

  Written finish() {
    result_.rssGrowth = peakRssGrowth(rssBase_);
    if (holder_ != nullptr) {
      result_.metrics = reported_;
      params_ = {};
      result_.poolPeak = holder_->rootPool()->peakBytes();
      holder_.reset();
    } else {
      if (shuffleWriter_ != nullptr) {
        result_.metrics = shuffleWriter_->metrics();
      }
      shuffleWriter_.reset();
      arrowPool_.reset();
      batches_.clear();
      result_.poolPeak = pool_->peakBytes();
    }
    return std::move(result_);
  }

 private:
  const Case& case_;
  const Writer writer_;
  Written result_;
  int64_t rssBase_{-1};
  // Direct.
  std::vector<RowVectorPtr> batches_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<BoltArrowMemoryPool> arrowPool_;
  std::shared_ptr<ShuffleWriter> shuffleWriter_;
  // Under a task memory limit.
  std::shared_ptr<memory::sparksql::test::TestMemoryManagerHolder> holder_;
  exec::test::CursorParameters params_;
  ShuffleWriterMetrics reported_;
};

/// An untimed lifecycle, for a read that finds no output of its writer.
Written write(const Case& c, Writer writer) {
  Lifecycle lifecycle(c, writer);
  lifecycle.prepare();
  lifecycle.run();
  return lifecycle.finish();
}

// ---- Readers ---------------------------------------------------------------

/// One arrow stream per partition slice of the in-memory data file.
class BufferStreamIterator final : public ReaderStreamIterator {
 public:
  BufferStreamIterator(
      const std::string* file,
      const std::vector<int64_t>* lengths)
      : file_(file), lengths_(lengths) {}

  std::shared_ptr<arrow::io::InputStream> nextStream(
      arrow::MemoryPool* /*pool*/) override {
    while (next_ < lengths_->size() && (*lengths_)[next_] == 0) {
      ++next_;
    }
    if (next_ == lengths_->size()) {
      return nullptr;
    }
    int64_t offset = 0;
    for (size_t i = 0; i < next_; ++i) {
      offset += (*lengths_)[i];
    }
    return std::make_shared<arrow::io::BufferReader>(
        reinterpret_cast<const uint8_t*>(file_->data()) + offset,
        (*lengths_)[next_++]);
  }

  void close() override {}
  void updateMetrics(
      int64_t,
      int64_t,
      int64_t,
      int64_t,
      int64_t,
      int64_t,
      int64_t,
      int64_t) override {}

 private:
  const std::string* const file_;
  const std::vector<int64_t>* const lengths_;
  size_t next_{0};
};

/// Delegating codec that meters decompression, the seam the cell reader
/// cannot meter itself.
class TimingCodec final : public Codec {
 public:
  explicit TimingCodec(Codec* inner)
      : Codec(
            fromArrowCodecType(benchmarkCodec()),
            CodecOptions{CodecBackend::NONE}),
        inner_(inner) {}

  int64_t compress(const uint8_t* in, int64_t n, uint8_t* out, int64_t cap)
      override {
    return inner_->compress(in, n, out, cap);
  }

  int64_t decompress(const uint8_t* in, int64_t n, uint8_t* out, int64_t cap)
      override {
    const uint64_t start = clockNs();
    const auto result = inner_->decompress(in, n, out, cap);
    decompressNs += clockNs() - start;
    return result;
  }

  int64_t maxCompressedLen(int64_t n) const override {
    return inner_->maxCompressedLen(n);
  }

  int32_t defaultCompressionLevel() const override {
    return inner_->defaultCompressionLevel();
  }

  uint64_t decompressNs{0};

 private:
  Codec* const inner_;
};

/// A written data file kept for the read benchmark that follows its write.
struct Output {
  std::string caseId;
  Writer writer;
  std::string file;
  std::vector<int64_t> partitionLengths;
  bool verified{false};
};

std::unique_ptr<Output>& residentOutput() {
  static std::unique_ptr<Output> current;
  return current;
}

struct Read {
  int64_t rows{0};
  uint64_t wallNs{0};
  uint64_t decompressNs{0};
  uint64_t decodeNs{0};
  uint64_t mergeNs{0};
  uint64_t checksum{0};
  int64_t poolPeak{0};
  int64_t rssGrowth{-1};
};

/// Reads every partition of `output`; `visit` sees each batch when given.
Read readBack(
    const Case& c,
    const Output& output,
    const std::function<void(const RowVector&)>& visit) {
  Read result;
  const auto rowType = dataType(c);
  auto pool = freshPool("reader");
  auto codec = createCodec(
      benchmarkCodec(),
      CodecOptions{CodecBackend::NONE, benchmarkCodecLevel(), true});
  const int64_t rssBase = resetPeakRss();
  if (isCell(output.writer)) {
    TimingCodec timing(codec.get());
    auto streams = std::make_shared<BufferStreamIterator>(
        &output.file, &output.partitionLengths);
    cell::CellShuffleReader reader(
        streams,
        rowType,
        &timing,
        arrow::default_memory_pool(),
        pool.get(),
        /*batchSize=*/4096,
        /*batchByteSize=*/1 << 20);
    const uint64_t start = clockNs();
    while (auto batch = reader.next()) {
      result.rows += batch->size();
      if (visit) {
        visit(*batch);
      }
    }
    result.wallNs = clockNs() - start;
    result.decompressNs = timing.decompressNs;
    result.decodeNs = reader.decodeTimeNs() - timing.decompressNs;
  } else {
    std::shared_ptr<Codec> shared = std::move(codec);
    BoltColumnarBatchDeserializerFactory factory(
        boltTypeToArrowSchema(rowType, pool.get()),
        shared,
        rowType,
        /*batchSize=*/4096,
        /*shuffleBatchByteSize=*/1 << 20,
        arrow::default_memory_pool(),
        pool.get());
    factory.setShuffleWriterType(writerType(output.writer));
    factory.setNumPartitions(c.spec.partitions);
    factory.setpartitioningShortName("hash");
    const uint64_t start = clockNs();
    int64_t offset = 0;
    for (const auto length : output.partitionLengths) {
      if (length == 0) {
        continue;
      }
      auto in = std::make_shared<arrow::io::BufferReader>(
          reinterpret_cast<const uint8_t*>(output.file.data()) + offset,
          length);
      offset += length;
      auto deserializer = factory.createDeserializer(std::move(in));
      while (auto batch = deserializer->next()) {
        result.rows += batch->size();
        if (visit) {
          visit(*batch);
        }
      }
    }
    result.wallNs = clockNs() - start;
    result.decompressNs = factory.getDecompressTime();
    result.decodeNs = factory.getDeserializeTime();
    result.mergeNs = factory.getMergeTime();
  }
  result.rssGrowth = peakRssGrowth(rssBase);
  result.poolPeak = pool->peakBytes();
  return result;
}

// ---- Benchmarks ------------------------------------------------------------

void setCounter(
    folly::UserCounters& counters,
    const char* name,
    int64_t value,
    folly::UserMetric::Type type = folly::UserMetric::Type::CUSTOM) {
  counters[name] = folly::UserMetric(value, type);
}

void writeCounters(
    folly::UserCounters& counters,
    const Case& c,
    const Written& w) {
  const auto& m = w.metrics;
  const auto time = folly::UserMetric::Type::TIME;
  const auto metric = folly::UserMetric::Type::METRIC;
  setCounter(counters, "ok", w.ok ? 1 : 0);
  setCounter(counters, "rows", w.rows);
  setCounter(counters, "input_bytes", c.inputBytes, metric);
  setCounter(counters, "wall_ns", w.wallNs, time);
  setCounter(counters, "split_ns", w.splitNs, time);
  setCounter(counters, "stop_ns", w.stopNs, time);
  setCounter(counters, "convert_ns", m.convertTime, time);
  setCounter(counters, "flatten_ns", m.flattenTime, time);
  setCounter(counters, "pid_ns", m.computePidTime, time);
  setCounter(counters, "combine_ns", m.combineVectorCost, time);
  setCounter(counters, "compress_ns", m.totalCompressTime, time);
  setCounter(counters, "write_ns", m.totalWriteTime, time);
  setCounter(counters, "evict_ns", m.totalEvictTime, time);
  setCounter(counters, "reclaim_ns", m.externalReclaimTime, time);
  setCounter(
      counters,
      "other_ns",
      static_cast<int64_t>(w.wallNs) - m.convertTime - m.flattenTime -
          m.computePidTime - m.combineVectorCost - m.totalCompressTime -
          m.totalWriteTime - m.totalEvictTime - m.externalReclaimTime,
      time);
  setCounter(counters, "bytes_written", m.totalBytesWritten, metric);
  setCounter(counters, "bytes_raw", m.dataSize, metric);
  setCounter(counters, "bytes_evicted", m.totalBytesEvicted, metric);
  setCounter(counters, "windows_or_spills", m.spillCount);
  setCounter(counters, "spill_files", m.totalSpillFiles);
  setCounter(counters, "dict_matched_rows", m.dictionaryMatchedRows);
  setCounter(counters, "dict_fallback_rows", m.dictionaryFallbackRows);
  setCounter(
      counters, "max_partition_buffer", m.maxPartitionBufferSize, metric);
  setCounter(counters, "avg_prealloc", m.avgPreallocSize);
  setCounter(counters, "combine_times", m.combineVectorTimes);
  setCounter(counters, "used_v2", m.useV2);
  setCounter(counters, "used_row_based", m.useRowBased);
  setCounter(counters, "pool_peak", w.poolPeak, metric);
  setCounter(counters, "writer_peak", m.peakBytes, metric);
  setCounter(counters, "rss_growth", w.rssGrowth, metric);
}

void readCounters(
    folly::UserCounters& counters,
    const Read& r,
    bool verifiedOk) {
  const auto time = folly::UserMetric::Type::TIME;
  const auto metric = folly::UserMetric::Type::METRIC;
  setCounter(counters, "ok", verifiedOk ? 1 : 0);
  setCounter(counters, "rows", r.rows);
  setCounter(counters, "wall_ns", r.wallNs, time);
  setCounter(counters, "decompress_ns", r.decompressNs, time);
  setCounter(counters, "decode_ns", r.decodeNs, time);
  setCounter(counters, "merge_ns", r.mergeNs, time);
  setCounter(
      counters,
      "other_ns",
      static_cast<int64_t>(r.wallNs - r.decompressNs - r.decodeNs - r.mergeNs),
      time);
  setCounter(counters, "pool_peak", r.poolPeak, metric);
  setCounter(counters, "rss_growth", r.rssGrowth, metric);
}

/// Keeps a successful write's file for the read benchmark and checks it,
/// once, against the input.
void keepOutput(const Case& c, Writer writer, const Written& w) {
  auto output = std::make_unique<Output>();
  output->caseId = c.spec.id;
  output->writer = writer;
  output->partitionLengths = w.metrics.partitionLengths;
  {
    std::ifstream in(w.dataFile, std::ios::binary);
    output->file.assign(
        std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  }
  if (FLAGS_bolt_benchmark_shuffle_verify) {
    uint64_t checksum = 0;
    const auto read = readBack(c, *output, [&](const RowVector& batch) {
      checksum += rowsHash(batch);
    });
    output->verified = read.rows == c.rows && checksum == c.checksum;
    if (!output->verified) {
      LOG(ERROR) << c.spec.name() << "/" << writerName(writer)
                 << ": output does not match the input (rows " << read.rows
                 << " of " << c.rows << ")";
    }
  } else {
    output->verified = true;
  }
  residentOutput() = std::move(output);
}

void removeFiles(const std::string& dataFile) {
  ::unlink(dataFile.c_str());
  ::unlink((dataFile + ".cellspill").c_str());
}

void registerCase(const CaseSpec& spec, const std::vector<Writer>& writers) {
  for (const auto writer : writers) {
    if (writer == Writer::kCellNoDict && !isDictionaryFamily(spec.family)) {
      continue;
    }
    if (writer == Writer::kCellUnsealedNulls && !hasHeavyNulls(spec)) {
      continue;
    }
    const std::string prefix = spec.name() + "/" + writerName(writer);
    folly::addBenchmark(
        __FILE__,
        prefix + "/write",
        [spec, writer](folly::UserCounters& counters, unsigned) -> unsigned {
          const Case* c = nullptr;
          std::unique_ptr<Lifecycle> lifecycle;
          BENCHMARK_SUSPEND {
            c = &getCase(spec);
            lifecycle = std::make_unique<Lifecycle>(*c, writer);
            lifecycle->prepare();
          }
          lifecycle->run();
          Written w;
          BENCHMARK_SUSPEND {
            w = lifecycle->finish();
            lifecycle.reset();
            writeCounters(counters, *c, w);
            if (w.ok) {
              keepOutput(*c, writer, w);
            } else {
              LOG(ERROR) << spec.name() << "/" << writerName(writer)
                         << " failed: " << w.error;
              residentOutput().reset();
            }
            removeFiles(w.dataFile);
          }
          return w.ok ? static_cast<unsigned>(w.rows) : 1;
        });
    folly::addBenchmark(
        __FILE__,
        prefix + "/read",
        [spec, writer](folly::UserCounters& counters, unsigned) -> unsigned {
          const Case* c = nullptr;
          const Output* output = nullptr;
          BENCHMARK_SUSPEND {
            c = &getCase(spec);
            auto& resident = residentOutput();
            if (resident == nullptr || resident->caseId != spec.id ||
                resident->writer != writer) {
              const auto w = write(*c, writer);
              if (w.ok) {
                keepOutput(*c, writer, w);
              } else {
                resident.reset();
              }
              removeFiles(w.dataFile);
            }
            output = resident.get();
          }
          if (output == nullptr) {
            BENCHMARK_SUSPEND {
              setCounter(counters, "ok", 0);
            }
            return 1;
          }
          const auto r = readBack(*c, *output, nullptr);
          BENCHMARK_SUSPEND {
            readCounters(counters, r, output->verified && r.rows == c->rows);
          }
          return static_cast<unsigned>(std::max<int64_t>(r.rows, 1));
        });
  }
}

std::vector<Writer> parseWriters() {
  std::vector<Writer> writers;
  std::stringstream in(FLAGS_bolt_benchmark_shuffle_writers);
  std::string name;
  bool cell = false;
  while (std::getline(in, name, ',')) {
    if (name == "Cell") {
      writers.push_back(Writer::kCell);
      cell = true;
    } else if (name == "V1") {
      writers.push_back(Writer::kV1);
    } else if (name == "V2") {
      writers.push_back(Writer::kV2);
    } else {
      BOLT_FAIL("unknown writer {}", name);
    }
  }
  if (cell) {
    writers.push_back(Writer::kCellNoDict);
    writers.push_back(Writer::kCellUnsealedNulls);
  }
  return writers;
}

uint32_t parseTier() {
  const auto& tier = FLAGS_bolt_benchmark_shuffle_tier;
  if (tier == "smoke") {
    return kSmoke;
  }
  if (tier == "standard") {
    return kStandard;
  }
  BOLT_CHECK_EQ(tier, "full", "unknown tier");
  return kFull;
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::Options options;
  options.allocatorCapacity = 16LL << 30;
  memory::MemoryManager::initialize(options);
  exec::Operator::registerOperator(
      std::make_unique<SparkShuffleWriterTranslator>());

  const uint32_t tier = parseTier();
  if (tier == kSmoke && FLAGS_bolt_benchmark_shuffle_input_mb == 0) {
    FLAGS_bolt_benchmark_shuffle_input_mb = 64;
  }
  const auto writers = parseWriters();
  // Memory-limited cases run last: the first one initializes the process's
  // Spark execution memory pool, which then stays, and a writer sizing from
  // it would see a limit an unlimited case does not have. The list keeps
  // the frozen order.
  auto specs = caseSpecs();
  if (!FLAGS_bolt_benchmark_shuffle_list) {
    std::stable_partition(specs.begin(), specs.end(), [](const auto& spec) {
      return spec.memLimitMb == 0;
    });
  }
  for (const auto& spec : specs) {
    if ((spec.tiers & tier) == 0) {
      continue;
    }
    if (FLAGS_bolt_benchmark_shuffle_list) {
      // A rate in per mille shows as its whole percent, and in full after
      // the other fields.
      printf(
          "%-16s %-24s P=%-6d nulls=%-3d input=%lldMB%s%s%s\n",
          spec.id.c_str(),
          familyName(spec.family),
          spec.partitions,
          spec.nullPerMille > 0 ? spec.nullPerMille / 10 : spec.nullPercent,
          static_cast<long long>(
              FLAGS_bolt_benchmark_shuffle_input_mb > 0
                  ? FLAGS_bolt_benchmark_shuffle_input_mb
                  : spec.inputMb),
          spec.skewed ? " skewed" : "",
          spec.memLimitMb > 0
              ? (" memLimit=" + std::to_string(spec.memLimitMb) + "MB").c_str()
              : "",
          spec.nullPerMille > 0
              ? (" nullPerMille=" + std::to_string(spec.nullPerMille)).c_str()
              : "");
      continue;
    }
    registerCase(spec, writers);
  }
  if (!FLAGS_bolt_benchmark_shuffle_list) {
    folly::runBenchmarks();
  }
  residentOutput().reset();
  residentCase().reset();
  return 0;
}
