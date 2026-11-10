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
/// steps behind that time - the writer's split, stop, convert, flatten
/// (Cell: shred), pid, combine, compress, write, evict and reclaim times,
/// bytes and windows, dictionary hits, memory peaks; the reader's
/// decompress, decode and merge times - so versions compare step by step.
/// --bm_json_verbose=<file> keeps them all as raw data.
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
///      complex types with nulls, four levels deep; BIGINT encoding
///      references at P=500: user ids and version codes as in
///      production, a hash-split sequence, event timestamps, wide random
///      values, clustered and 33-40-bit ids repeating with Zipf frequencies
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
///      and the mix; a few hot partitions taking most of an event log's
///      rows, each sealed past partitionSealBytes with memory to spare
///   F  a 256 MiB task memory limit enforced by the Spark memory manager,
///      so every writer spills through reclaim: ints, medium and long
///      strings, dictionary strings; the complex types, nested nulls, 50%
///      nulls, optional fields; the production user-id shape
///
/// --bolt_benchmark_shuffle_tier picks the set: smoke (A1, A5, A7, A8 at
/// 64 MiB), standard (A, B, C and F11) or full (A to F). --bm_regex
/// narrows it.
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
#include <sstream>
#include <unordered_set>

#include <arrow/io/memory.h>
#include <fmt/format.h>

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
    "the dictionary cases, CellUnsealedNulls, which never seals a window "
    "for its null memory, the cases with heavy nulls, and CellAdapter, which "
    "packs nested columns through the type adapter, the nested cases.");
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
DEFINE_int64(
    bolt_benchmark_shuffle_cell_spill_memory_bytes,
    -1,
    "Cell writers' spillMemoryBytes: the newest spill data kept in memory, "
    "at most this many bytes; -1 no cap, 0 none.");
DEFINE_int64(
    bolt_benchmark_shuffle_cell_spill_bytes_per_stream,
    512 << 10,
    "Cell writers' cellSpillBytesPerStream: past this much cell data for "
    "every stream of every partition, they spill without memory pressure.");

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
  kStrDate8,
  kStrDate10,
  kEvents,
  kUidZipf,
  kSeqHash,
  kTsEvent,
  kWideRandom,
  kClusteredIds,
  kIds33To40Bit,
};

/// The families an extra Cell writer joins: CellNoDict the dictionary ones,
/// with strings or complex values few enough for a string dictionary, and
/// CellAdapter the nested ones, with ARRAY, MAP or ROW columns.
enum FamilyTrait : uint32_t { kDictionary = 1, kNested = 2 };

struct FamilyInfo {
  Family family;
  const char* name;
  uint32_t traits{0};
};

constexpr FamilyInfo kFamilies[] = {
    {Family::kIntNarrow, "int_narrow"},
    {Family::kIntRandom, "int_random"},
    {Family::kDouble, "double"},
    {Family::kStrShort, "str_short"},
    {Family::kStrMedium, "str_medium"},
    {Family::kStrLong, "str_long"},
    {Family::kStrLowCard, "str_lowcard", kDictionary},
    {Family::kStrLowCardClustered, "str_lowcard_clustered", kDictionary},
    {Family::kMixed, "mixed"},
    {Family::kWide, "wide100"},
    {Family::kComplex, "complex", kNested},
    {Family::kProd, "prod", kDictionary},
    {Family::kWideSparse, "wide100_sparse_nulls"},
    {Family::kDeadColumn, "int_narrow_dead_column"},
    {Family::kStaleNulls, "int_narrow_stale_nulls"},
    {Family::kStrRealistic, "str_realistic"},
    {Family::kQ22Rollup, "q22_rollup", kDictionary},
    {Family::kDeadWide, "int_narrow_dead_wide"},
    {Family::kWideOptional, "wide50_optional"},
    {Family::kArrayStruct, "array_struct", kNested},
    {Family::kMapStringArray, "map_str_array", kNested},
    {Family::kArrayDouble, "array_double128", kNested},
    {Family::kArrayBigint, "array_bigint1k", kNested},
    {Family::kDeepNested, "array_map_array_struct", kNested},
    {Family::kArrayIntNulls, "array_int1k", kNested},
    {Family::kNestedOptional, "array_struct50_optional", kNested},
    {Family::kNestedDense, "array_struct20", kNested},
    {Family::kNestedDead, "array_struct_dead32", kNested},
    {Family::kStructDense, "struct50", kNested},
    {Family::kStructChain, "struct_chain8", kNested},
    {Family::kLowCardComplex, "lowcard_complex", kDictionary | kNested},
    {Family::kContainers, "array_map", kNested},
    {Family::kArraysOptional, "arrays50_optional", kNested},
    {Family::kLowCardArray, "lowcard_array", kDictionary | kNested},
    {Family::kStrDate8, "str_date8", kDictionary},
    {Family::kStrDate10, "str_date10", kDictionary},
    {Family::kEvents, "events"},
    {Family::kUidZipf, "uid_zipf"},
    {Family::kSeqHash, "seq_hash"},
    {Family::kTsEvent, "ts_event"},
    {Family::kWideRandom, "wide_random"},
    {Family::kClusteredIds, "clustered_ids"},
    {Family::kIds33To40Bit, "ids_33_40bit"},
};

const FamilyInfo& familyInfo(Family family) {
  for (const auto& info : kFamilies) {
    if (info.family == family) {
      return info;
    }
  }
  BOLT_UNREACHABLE();
}

bool hasTrait(Family family, FamilyTrait trait) {
  return (familyInfo(family).traits & trait) != 0;
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
  /// Skewed another way: this many partitions, spread over the range, take
  /// nine rows in ten between them.
  int32_t hotPartitions{0};

  std::string name() const {
    return id + "." + familyInfo(family).name;
  }
};

/// The cases whose nulls can fill the null state, which CellUnsealedNulls
/// joins: the nested ones with nulls at some level, and the flat ones with
/// nulls in half the rows or more of several columns.
bool hasHeavyNulls(const CaseSpec& spec) {
  if (hasTrait(spec.family, kNested)) {
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
  // The full tier under a 256 MiB task memory limit.
  const auto limited = [&](std::string id,
                           Family family,
                           int32_t partitions,
                           int32_t nulls = 0) {
    add(std::move(id), family, partitions, kFull, nulls, 1024, false, 256);
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
  limited("F1", Family::kIntNarrow, 1024);
  limited("F5", Family::kStrMedium, 1024);
  limited("F6", Family::kStrLong, 1024);
  limited("F7", Family::kStrLowCard, 1024);
  // Compressible data: strings with real structure, and TPC-DS q22's rollup
  // shuffle. Spark's default 200 partitions for q22.
  add("A12", Family::kStrRealistic, 1024, kStd);
  add("A13", Family::kQ22Rollup, 200, kStd);
  limited("F8", Family::kStrRealistic, 1024);
  limited("F9", Family::kQ22Rollup, 200);
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
  limited("F10", Family::kComplex, 1024);
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
    limited(
        std::string("F-array-nulls-") + suffix, Family::kArrayIntNulls, 1024);
    c.back().nullPerMille = perMille;
  }
  // The optional fields of a wide table in an array of structs, the fields
  // of dense structs with nulls, and structs of fields null throughout.
  add("C-nested-optional", Family::kNestedOptional, 1024, kStd);
  add("C-nested-optional-P30000", Family::kNestedOptional, 30000, kStd);
  limited("F-nested-optional-P30000", Family::kNestedOptional, 30000);
  add("C-nested-dense", Family::kNestedDense, 1024, kStd, 10);
  add("C-nested-dense-P30000", Family::kNestedDense, 30000, kStd, 10);
  limited("F-nested-dense", Family::kNestedDense, 1024, 10);
  limited("F-nested-dense-P30000", Family::kNestedDense, 30000, 10);
  add("C-nested-dead", Family::kNestedDead, 1024, kStd);
  add("C-nested-dead-P30000", Family::kNestedDead, 30000, kStd);
  // The null-heavy flat shapes under the memory limit.
  limited("F-C1-N50", Family::kIntNarrow, 1024, 50);
  limited("F-wide-optional", Family::kWideOptional, 1024);
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
  limited("F-arrays-optional-P30000", Family::kArraysOptional, 30000);
  // Arrays of four values few and small enough for a string dictionary.
  add("C-lowcard-array", Family::kLowCardArray, 1024, kStd);
  // Five dates as strings, "20261102" and "2026-11-02": values of 8 and 10
  // characters that a string dictionary holds.
  add("A7-date8", Family::kStrDate8, 1024, kStd);
  add("A7-date10", Family::kStrDate10, 1024, kStd);
  // Six of 500 partitions take 90% of an event log's rows: each passes
  // partitionSealBytes once, with memory to spare, as the hot partitions of
  // skewed stages do.
  add("E-skew-seal", Family::kEvents, 500, kFull, 0, 600);
  c.back().hotPartitions = 6;
  // BIGINT encoding references: 500 partitions, synthetic values.
  // The production shape: user ids of 11 to 16 digits, Zipf per partition,
  // beside 8-digit version codes.
  add("A20", Family::kUidZipf, 500, kStd);
  // Auto-increment ids hashed to partitions: guards FOR on hash-split
  // sequences.
  add("A21", Family::kSeqHash, 500, kStd);
  // Millisecond event times in arrival order, minutes of jitter, random
  // partitions: guards FOR on timestamps.
  add("A22", Family::kTsEvent, 500, kStd);
  // Distinct random values in [2^40, 2^47), random partitions: the known
  // cost of the 48-bit kPlain cutoff.
  add("A23", Family::kWideRandom, 500, kStd);
  // Repeated ids clustered in [1.1e12, 1.9e12), Zipf per partition: guards
  // the FOR tie at 40-bit residuals.
  add("A24", Family::kClusteredIds, 500, kStd);
  // Repeated ids of 33 to 40 bits, Zipf per partition: the remaining gap,
  // 5-byte fields under zstd's minMatch of 7.
  add("A25", Family::kIds33To40Bit, 500, kStd);
  // The production shape under the 256 MiB limit: the spill path seen in
  // production.
  add("F11", Family::kUidZipf, 500, kStd, 0, 1024, false, 256);
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

/// Nulls of `n` rows, each null at `rate` in `scale`; nullptr at rate 0,
/// which draws nothing from the generator.
BufferPtr randomNulls(vector_size_t n, int32_t rate, int32_t scale, Rng& rng) {
  if (rate == 0) {
    return nullptr;
  }
  auto nulls = AlignedBuffer::allocate<bool>(n, inputPool(), bits::kNotNull);
  auto* raw = nulls->asMutable<uint64_t>();
  for (vector_size_t i = 0; i < n; ++i) {
    if (static_cast<int32_t>(rng() % scale) < rate) {
      bits::setNull(raw, i);
    }
  }
  return nulls;
}

BufferPtr nullsFor(vector_size_t n, int32_t nullPercent, Rng& rng) {
  return randomNulls(n, nullPercent, 100, rng);
}

BufferPtr nullsPerMille(vector_size_t n, int32_t perMille, Rng& rng) {
  return randomNulls(n, perMille, 1000, rng);
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

void randomChars(Rng& rng, size_t length, std::string& out) {
  out.resize(length);
  for (auto& c : out) {
    c = static_cast<char>('a' + rng() % 26);
  }
}

/// `n` integers drawn below `range`, a batch of them by default.
template <typename T>
VectorPtr randomInts(
    const TypePtr& type,
    int32_t nullPercent,
    Rng& rng,
    uint64_t range,
    vector_size_t n = kRowsPerBatch) {
  return flat<T>(
      type,
      nullPercent,
      rng,
      [&](vector_size_t) { return static_cast<T>(rng() % range); },
      n);
}

/// `n` prices below 10'000 with cents, a batch of them by default.
VectorPtr
prices(int32_t nullPercent, Rng& rng, vector_size_t n = kRowsPerBatch) {
  return flat<double>(
      DOUBLE(),
      nullPercent,
      rng,
      [&](vector_size_t) { return (rng() % 1'000'000) / 100.0; },
      n);
}

/// `n` strings of `low` to `high` - 1 random letters, a batch of them by
/// default.
VectorPtr randomStrings(
    int32_t nullPercent,
    Rng& rng,
    size_t low,
    size_t high,
    vector_size_t n = kRowsPerBatch) {
  return strings(
      nullPercent,
      rng,
      [&](vector_size_t, std::string& out) {
        randomChars(rng, low + rng() % (high - low), out);
      },
      VARCHAR(),
      n);
}

/// `n` strings drawn from `words`, a batch of them by default.
VectorPtr randomWords(
    int32_t nullPercent,
    Rng& rng,
    const std::vector<std::string>& words,
    vector_size_t n = kRowsPerBatch) {
  return strings(
      nullPercent,
      rng,
      [&](vector_size_t, std::string& out) {
        out = words[rng() % words.size()];
      },
      VARCHAR(),
      n);
}

/// Field `col` of 50 optional ones: bigints from `first` + `col` up, the
/// first 25 fields null in 0.1% of the rows and the others in 99.9%.
VectorPtr optionalField(int64_t first, int col, vector_size_t n, Rng& rng) {
  auto field = flat<int64_t>(
      BIGINT(), 0, rng, [&](vector_size_t i) { return first + i + col; }, n);
  field->setNulls(nullsPerMille(n, col < 25 ? 1 : 999, rng));
  return field;
}

/// `n` zeros of `type`: BIGINT, INTEGER, DOUBLE or VARCHAR, whose zero is
/// the empty string.
VectorPtr
zeros(const TypePtr& type, int32_t nullPercent, Rng& rng, vector_size_t n) {
  switch (type->kind()) {
    case TypeKind::BIGINT:
      return flat<int64_t>(
          type, nullPercent, rng, [](vector_size_t) { return int64_t{0}; }, n);
    case TypeKind::INTEGER:
      return flat<int32_t>(
          type, nullPercent, rng, [](vector_size_t) { return int32_t{0}; }, n);
    case TypeKind::DOUBLE:
      return flat<double>(
          type, nullPercent, rng, [](vector_size_t) { return 0.0; }, n);
    default:
      return strings(
          nullPercent, rng, [](vector_size_t, std::string&) {}, type, n);
  }
}

/// Appends 32 columns of `n` rows null in every row, cycling through four
/// types: constant nulls, and the flat vectors `allNull` makes.
void addDeadColumns(
    vector_size_t n,
    const std::function<VectorPtr(const TypePtr&)>& allNull,
    std::vector<std::string>& names,
    std::vector<VectorPtr>& columns) {
  const std::array<TypePtr, 4> types{BIGINT(), INTEGER(), DOUBLE(), VARCHAR()};
  for (int col = 0; col < 32; ++col) {
    names.push_back("dead" + std::to_string(col));
    const auto& type = types[(col / 2) % types.size()];
    if (col % 2 == 0) {
      columns.push_back(BaseVector::createNullConstant(type, n, inputPool()));
    } else {
      columns.push_back(allNull(type));
    }
  }
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

RowVectorPtr structOf(
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

RowVectorPtr makeRow(
    std::vector<std::string> names,
    std::vector<VectorPtr> children) {
  return structOf(
      std::move(names), std::move(children), nullptr, kRowsPerBatch);
}

/// Five consecutive dates as 8-character "yyyyMMdd" strings, or as
/// 10-character "yyyy-MM-dd" ones.
const std::vector<std::string>& dateVocabulary(bool dashed) {
  static const auto make = [](bool withDashes) {
    std::vector<std::string> dates;
    for (int day = 1; day <= 5; ++day) {
      dates.push_back(
          (withDashes ? "2026-11-0" : "2026110") + std::to_string(day));
    }
    return dates;
  };
  static const std::vector<std::string> plain = make(false);
  static const std::vector<std::string> dashes = make(true);
  return dashed ? dashes : plain;
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
    // Hot partitions: nine rows in ten go to one of them.
    if (spec.hotPartitions > 0 && rng() % 10 != 0) {
      return static_cast<int32_t>(
          rng() % spec.hotPartitions * (p / spec.hotPartitions));
    }
    return static_cast<int32_t>(rng() % p);
  });
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
      randomInts<int32_t>(INTEGER(), nulls, rng, 1000),
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

/// Column `col` of the wide family, which cycles through ten types.
VectorPtr wideColumn(int col, int32_t nulls, Rng& rng) {
  switch (col % 10) {
    case 0:
      return randomInts<int64_t>(BIGINT(), nulls, rng, 1'000'000'000);
    case 1:
      return randomInts<int32_t>(INTEGER(), nulls, rng, 100'000);
    case 2:
      return flat<double>(DOUBLE(), nulls, rng, [&](vector_size_t) {
        return (rng() % 10'000'000) / 1000.0;
      });
    case 3:
      return randomStrings(nulls, rng, 8, 25);
    case 4:
      return flat<int32_t>(DATE(), nulls, rng, [&](vector_size_t) {
        return static_cast<int32_t>(18000 + rng() % 2000);
      });
    case 5:
      return randomInts<int64_t>(DECIMAL(7, 2), nulls, rng, 1'000'000);
    case 6:
      return flat<bool>(
          BOOLEAN(), nulls, rng, [&](vector_size_t) { return rng() % 2 == 0; });
    case 7:
      // Microsecond precision: what every writer keeps.
      return flat<Timestamp>(TIMESTAMP(), nulls, rng, [&](vector_size_t) {
        return Timestamp(
            1'700'000'000 + static_cast<int64_t>(rng() % 100'000'000),
            (rng() % 1'000'000) * 1000);
      });
    case 8:
      return flat<int128_t>(DECIMAL(20, 2), nulls, rng, [&](vector_size_t) {
        return static_cast<int128_t>(rng() % 1'000'000'000'000ULL) * 1000;
      });
    default:
      return randomInts<int16_t>(SMALLINT(), nulls, rng, 30'000);
  }
}

// ---- BIGINT encoding references --------------------------------------------
//
// Synthetic values only. The id pools are drawn once, on first use, from
// fixed seeds of their own, so a family's cases share them; each case's
// rows are drawn from its own generator.

/// The partition of `id` among `partitions` under a hash partitioning.
int32_t hashPartition(int64_t id, int32_t partitions) {
  return static_cast<int32_t>(
      folly::hash::twang_mix64(static_cast<uint64_t>(id)) % partitions);
}

/// A double in [0, 1) from one draw, the same with every standard library.
double unitInterval(Rng& rng) {
  return static_cast<double>(rng() >> 11) * 0x1.0p-53;
}

/// Ids repeating with Zipf frequencies (s = 1) within their partitions:
/// each partition holds the ids that hash to it, the k-th drawn of them
/// 1/k as frequent as the first.
struct ZipfIds {
  /// Per partition: its ids in the order drawn, their cumulative weights.
  std::vector<std::vector<int64_t>> ids;
  std::vector<std::vector<double>> cumulative;

  /// A row's partition, uniform, and the index of its id there, by the
  /// Zipf law.
  std::pair<int32_t, size_t> sample(Rng& rng) const {
    const auto partition = static_cast<int32_t>(rng() % ids.size());
    const auto& weights = cumulative[partition];
    const double at = unitInterval(rng) * weights.back();
    const auto index = static_cast<size_t>(
        std::upper_bound(weights.begin(), weights.end(), at) - weights.begin());
    return {partition, std::min(index, weights.size() - 1)};
  }
};

/// `perPartition` times `partitions` distinct ids, drawn by `draw` from a
/// generator seeded with `seed`: about `perPartition` to a partition.
ZipfIds zipfIds(
    int32_t partitions,
    int32_t perPartition,
    uint64_t seed,
    const std::function<int64_t(Rng&)>& draw) {
  ZipfIds result;
  result.ids.resize(partitions);
  result.cumulative.resize(partitions);
  Rng rng(seed);
  std::unordered_set<int64_t> seen;
  const auto total = static_cast<size_t>(partitions) * perPartition;
  while (seen.size() < total) {
    const int64_t id = draw(rng);
    if (seen.insert(id).second) {
      result.ids[hashPartition(id, partitions)].push_back(id);
    }
  }
  for (int32_t p = 0; p < partitions; ++p) {
    BOLT_CHECK(!result.ids[p].empty(), "no id hashes to partition {}", p);
    double sum = 0;
    for (size_t k = 1; k <= result.ids[p].size(); ++k) {
      sum += 1.0 / static_cast<double>(k);
      result.cumulative[p].push_back(sum);
    }
  }
  return result;
}

/// The uid_zipf family's users and versions: user ids of 11 to 16 digits,
/// each length as likely, about 1000 to a partition; 2000 version codes.
struct Users {
  ZipfIds ids;
  /// Per partition and user, the index of its primary version.
  std::vector<std::vector<uint16_t>> primary;
  /// 1500 codes in [18.0M, 18.9M), then 500 in [9M, 18M), as 8 digits,
  /// zero-padded.
  std::vector<std::string> versions;
};

/// A version's index: one of the 1500 high codes, uniformly, 85 times in
/// 100, else one of the 500 low ones.
uint16_t versionIndex(Rng& rng) {
  return static_cast<uint16_t>(
      rng() % 100 < 85 ? rng() % 1500 : 1500 + rng() % 500);
}

const Users& uidUsers(int32_t partitions) {
  static const Users users = [&] {
    Users built;
    built.ids = zipfIds(partitions, 1000, 0xA20, [](Rng& rng) {
      const auto digits = 11 + static_cast<int32_t>(rng() % 6);
      int64_t low = 1;
      for (int32_t d = 1; d < digits; ++d) {
        low *= 10;
      }
      return low + static_cast<int64_t>(rng() % static_cast<uint64_t>(9 * low));
    });
    Rng rng(0xA20 + 1);
    std::unordered_set<int64_t> seen;
    std::vector<int64_t> codes;
    while (codes.size() < 2000) {
      const int64_t code = codes.size() < 1500
          ? 18'000'000 + static_cast<int64_t>(rng() % 900'000)
          : 9'000'000 + static_cast<int64_t>(rng() % 9'000'000);
      if (seen.insert(code).second) {
        codes.push_back(code);
      }
    }
    for (const auto code : codes) {
      built.versions.push_back(fmt::format("{:08d}", code));
    }
    built.primary.resize(partitions);
    for (int32_t p = 0; p < partitions; ++p) {
      for (size_t k = 0; k < built.ids.ids[p].size(); ++k) {
        built.primary[p].push_back(versionIndex(rng));
      }
    }
    return built;
  }();
  BOLT_CHECK_EQ(users.ids.ids.size(), partitions);
  return users;
}

/// The clustered_ids family's ids: in [1.1e12, 1.9e12), about 1000 to a
/// partition.
const ZipfIds& clusteredIds(int32_t partitions) {
  static const ZipfIds ids = zipfIds(partitions, 1000, 0xA24, [](Rng& rng) {
    return 1'100'000'000'000LL +
        static_cast<int64_t>(rng() % 800'000'000'000ULL);
  });
  BOLT_CHECK_EQ(ids.ids.size(), partitions);
  return ids;
}

/// The ids_33_40bit family's ids: of 33 to 40 bits, each length as likely,
/// about 1000 to a partition.
const ZipfIds& ids33To40Bit(int32_t partitions) {
  static const ZipfIds ids = zipfIds(partitions, 1000, 0xA25, [](Rng& rng) {
    const auto bits = 33 + static_cast<int32_t>(rng() % 8);
    const int64_t low = int64_t{1} << (bits - 1);
    return low + static_cast<int64_t>(rng() % static_cast<uint64_t>(low));
  });
  BOLT_CHECK_EQ(ids.ids.size(), partitions);
  return ids;
}

/// The wide_random family's value of row `row`: in [2^40, 2^47), distinct
/// for every row below 2^47 - 2^40. A bijection of [0, 2^47) - odd
/// multiplications and xor-shifts - walked until it lands below
/// 2^47 - 2^40.
int64_t wideRandomValue(int64_t row) {
  constexpr uint64_t kMask = (uint64_t{1} << 47) - 1;
  constexpr uint64_t kSpan = (uint64_t{1} << 47) - (uint64_t{1} << 40);
  uint64_t x = static_cast<uint64_t>(row);
  do {
    x = (x * 0x9E3779B97F4A7C15ULL) & kMask;
    x ^= x >> 23;
    x = (x * 0xBF58476D1CE4E5B9ULL) & kMask;
    x ^= x >> 29;
  } while (x >= kSpan);
  return static_cast<int64_t>((uint64_t{1} << 40) + x);
}

/// The first id of the seq_hash family's sequence.
constexpr int64_t kFirstSequenceId = 6'000'000'000LL;

/// The ts_event family's first arrival, 2026-01-01T00:00:00Z in
/// milliseconds; three rows arrive a millisecond.
constexpr int64_t kFirstArrivalMs = 1'767'225'600'000LL;

/// A BIGINT column of `n` rows, row i `value(i)`.
VectorPtr bigints(
    Rng& rng,
    const std::function<int64_t(vector_size_t)>& value,
    vector_size_t n = kRowsPerBatch) {
  return flat<int64_t>(BIGINT(), 0, rng, value, n);
}

/// A pid column, row i in partition `partition[i]`.
VectorPtr partitionColumn(Rng& rng, const std::vector<int32_t>& partition) {
  return flat<int32_t>(
      INTEGER(),
      0,
      rng,
      [&](vector_size_t i) { return partition[i]; },
      static_cast<vector_size_t>(partition.size()));
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
    case Family::kStaleNulls:
    case Family::kDeadColumn:
    case Family::kDeadWide: {
      auto columns = intNarrowColumns(
          batch, nulls, rng, spec.family == Family::kStaleNulls);
      columns.insert(columns.begin(), pid);
      std::vector<std::string> names{"pid", "id", "ts", "small", "rnd"};
      if (spec.family == Family::kDeadColumn) {
        names.push_back("dead");
        columns.push_back(BaseVector::createNullConstant(
            BIGINT(), kRowsPerBatch, inputPool()));
      } else if (spec.family == Family::kDeadWide) {
        // Flat dead columns whose nulls are drawn at 100%.
        addDeadColumns(
            kRowsPerBatch,
            [&](const TypePtr& type) {
              return zeros(type, 100, rng, kRowsPerBatch);
            },
            names,
            columns);
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
           prices(nulls, rng),
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
      return makeRow(
          {"pid", "key", "value"},
          {pid, key(), randomStrings(nulls, rng, 8, 32)});
    case Family::kStrMedium:
      return makeRow(
          {"pid", "key", "value"},
          {pid, key(), randomStrings(nulls, rng, 8, 100)});
    case Family::kStrLong:
      return makeRow(
          {"pid", "key", "value"},
          {pid, key(), randomStrings(nulls, rng, 200, 2000)});
    case Family::kStrLowCard:
      return makeRow(
          {"pid", "key", "value"},
          {pid, key(), randomWords(nulls, rng, lowCardVocabulary())});
    case Family::kStrDate8:
    case Family::kStrDate10:
      return makeRow(
          {"pid", "key", "value"},
          {pid,
           key(),
           randomWords(
               nulls, rng, dateVocabulary(spec.family == Family::kStrDate10))});
    case Family::kStrLowCardClustered: {
      // Runs of 64 rows of one word.
      const auto& vocab = lowCardVocabulary();
      return makeRow(
          {"pid", "key", "value"},
          {pid,
           key(),
           strings(nulls, rng, [&](vector_size_t i, std::string& out) {
             out = vocab[(i / 64) % vocab.size()];
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
           randomInts<int64_t>(BIGINT(), nulls, rng, 300'000),
           randomInts<int64_t>(BIGINT(), nulls, rng, 12'000'000),
           randomInts<int32_t>(INTEGER(), nulls, rng, 400),
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
           randomWords(nulls, rng, promotions),
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
        columns.push_back(wideColumn(col, nulls, rng));
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
        columns.push_back(optionalField(
            int64_t{batch} * kRowsPerBatch, col, kRowsPerBatch, rng));
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
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        arraySizes[i] = rng() % 9;
        mapSizes[i] = rng() % 5;
      }
      const auto arrayAt = entries(arraySizes);
      const auto mapAt = entries(mapSizes);
      // One draw from the generator per statement, so the order of the
      // draws is the order of the code.
      auto arrayNulls = nullsFor(kRowsPerBatch, nulls, rng);
      auto elements =
          randomInts<int64_t>(BIGINT(), nulls, rng, 100'000, arrayAt.total);
      auto array =
          arrayOf(kRowsPerBatch, std::move(arrayNulls), arrayAt, elements);
      auto mapNulls = nullsFor(kRowsPerBatch, nulls, rng);
      auto keys = flat<int32_t>(
          INTEGER(),
          0,
          rng,
          [](vector_size_t i) { return static_cast<int32_t>(i % 5); },
          mapAt.total);
      auto values = flat<int64_t>(
          BIGINT(),
          nulls,
          rng,
          [&](vector_size_t) { return static_cast<int64_t>(rng()); },
          mapAt.total);
      auto map = mapOf(kRowsPerBatch, std::move(mapNulls), mapAt, keys, values);
      if (spec.family == Family::kContainers) {
        return makeRow({"pid", "key", "arr", "map"}, {pid, key(), array, map});
      }
      auto structNulls = nullsFor(kRowsPerBatch, nulls, rng);
      auto a = randomInts<int32_t>(INTEGER(), nulls, rng, 1000);
      auto b = randomStrings(nulls, rng, 4, 13);
      auto structure =
          structOf({"a", "b"}, {a, b}, std::move(structNulls), kRowsPerBatch);
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
          {pid, wrap(key()), wrap(randomWords(10, rng, vocab))});
    }
    // The nested families draw from the generator once per statement, so
    // the order of the draws is the order of the code.
    case Family::kArrayStruct: {
      // An array of 0 to 8 structs of an int and a short string.
      const auto at = entries(kRowsPerBatch, 0, 8, rng);
      auto a = randomInts<int32_t>(INTEGER(), 0, rng, 1000, at.total);
      auto b = randomStrings(0, rng, 4, 13, at.total);
      auto elements = structOf({"a", "b"}, {a, b}, nullptr, at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, elements);
      return makeRow({"pid", "key", "arr"}, {pid, key(), array});
    }
    case Family::kMapStringArray: {
      // A map of 0 to 4 low-cardinality words to arrays of 0 to 8 bigints.
      const auto mapAt = entries(kRowsPerBatch, 0, 4, rng);
      auto keys = randomWords(0, rng, lowCardVocabulary(), mapAt.total);
      const auto arrayAt = entries(mapAt.total, 0, 8, rng);
      auto elements =
          randomInts<int64_t>(BIGINT(), 0, rng, 100'000, arrayAt.total);
      auto values = arrayOf(mapAt.total, nullptr, arrayAt, elements);
      auto map = mapOf(kRowsPerBatch, nullptr, mapAt, keys, values);
      return makeRow({"pid", "key", "map"}, {pid, key(), map});
    }
    case Family::kArrayDouble: {
      // An array of 128 prices.
      const auto at = entries(kRowsPerBatch, 128, 128, rng);
      auto values = prices(0, rng, at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, values);
      return makeRow({"pid", "key", "prices"}, {pid, key(), array});
    }
    case Family::kArrayBigint: {
      // An array of 900 to 1100 bigints.
      const auto at = entries(kRowsPerBatch, 900, 1100, rng);
      auto values = randomInts<int64_t>(BIGINT(), 0, rng, 100'000, at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, values);
      return makeRow({"pid", "key", "values"}, {pid, key(), array});
    }
    case Family::kDeepNested: {
      // Four levels: an array of maps of low-cardinality words to arrays
      // of structs of an int and a timestamp, each level 0 to 3 long, with
      // nulls at every level but the map keys.
      const auto outerAt = entries(kRowsPerBatch, 0, 3, rng);
      auto outerNulls = nullsFor(kRowsPerBatch, nulls, rng);
      const auto mapAt = entries(outerAt.total, 0, 3, rng);
      auto mapNulls = nullsFor(outerAt.total, nulls, rng);
      auto keys = randomWords(0, rng, lowCardVocabulary(), mapAt.total);
      const auto innerAt = entries(mapAt.total, 0, 3, rng);
      auto innerNulls = nullsFor(mapAt.total, nulls, rng);
      auto structNulls = nullsFor(innerAt.total, nulls, rng);
      auto x = randomInts<int32_t>(INTEGER(), nulls, rng, 1000, innerAt.total);
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
      auto values = randomInts<int32_t>(INTEGER(), 0, rng, 100'000, at.total);
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
        fields.push_back(optionalField(
            int64_t{batch} * kRowsPerBatch * 8, col, at.total, rng));
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
        fields.push_back(
            randomInts<int64_t>(BIGINT(), nulls, rng, 100'000, at.total));
      }
      auto structs =
          structOf(std::move(names), std::move(fields), nullptr, at.total);
      auto array = arrayOf(kRowsPerBatch, nullptr, at, structs);
      return makeRow({"pid", "key", "arr"}, {pid, key(), array});
    }
    case Family::kNestedDead: {
      // An array of 0 to 8 structs of four fields with values and 32 null
      // in every row, the flat ones through an all-null buffer.
      const auto at = entries(kRowsPerBatch, 0, 8, rng);
      std::vector<std::string> names{"a", "b", "c", "d"};
      std::vector<VectorPtr> fields{
          randomInts<int32_t>(INTEGER(), 0, rng, 1000, at.total),
          randomInts<int64_t>(BIGINT(), 0, rng, 100'000, at.total),
          prices(0, rng, at.total),
          randomStrings(0, rng, 4, 13, at.total)};
      addDeadColumns(
          at.total,
          [&](const TypePtr& type) {
            auto dead = zeros(type, 0, rng, at.total);
            dead->setNulls(AlignedBuffer::allocate<bool>(
                at.total, inputPool(), bits::kNull));
            return dead;
          },
          names,
          fields);
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
        fields.push_back(randomInts<int64_t>(BIGINT(), 0, rng, 100'000));
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
            randomInts<int64_t>(BIGINT(), 0, rng, 100'000)};
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
        auto values = optionalField(
            int64_t{batch} * kRowsPerBatch * 8, col, at.total, rng);
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
    case Family::kEvents: {
      // An event log: a sequential id, a user, an event type, the page
      // and a detail of a few words. The strings repeat in fragments, not
      // whole, so no dictionary takes them; LZ4 takes the rows to about
      // half.
      static const std::vector<std::string> sections = {
          "home",
          "search",
          "item",
          "cart",
          "checkout",
          "account",
          "orders",
          "help",
          "deals",
          "brands",
          "reviews",
          "wishlist",
          "settings",
          "payment",
          "returns",
          "stores"};
      static const std::vector<std::string> words = {
          "click",  "view",    "scroll",  "open",   "close", "add",
          "remove", "update",  "submit",  "cancel", "retry", "share",
          "banner", "button",  "link",    "image",  "video", "price",
          "filter", "sort",    "page",    "next",   "back",  "menu",
          "mobile", "desktop", "tablet",  "app",    "web",   "guest",
          "member", "premium", "trial",   "coupon", "gift",  "card",
          "fast",   "slow",    "timeout", "error",  "ok",    "done"};
      return makeRow(
          {"pid", "id", "user", "type", "page", "detail"},
          {pid,
           flat<int64_t>(
               BIGINT(),
               0,
               rng,
               [&](vector_size_t i) {
                 return 9'000'000'000LL + int64_t{batch} * kRowsPerBatch + i;
               }),
           randomInts<int64_t>(BIGINT(), 0, rng, 20'000'000),
           randomInts<int32_t>(INTEGER(), 0, rng, 40),
           strings(
               0,
               rng,
               [&](vector_size_t, std::string& out) {
                 out = "/" + sections[rng() % sections.size()] + "/" +
                     sections[rng() % sections.size()] + "/" +
                     std::to_string(rng() % 1'000'000);
               }),
           strings(0, rng, [&](vector_size_t, std::string& out) {
             out.clear();
             const int numWords = 6 + rng() % 9;
             for (int w = 0; w < numWords; ++w) {
               if (w > 0) {
                 out += ' ';
               }
               out += words[rng() % words.size()];
             }
           })});
    }
    case Family::kUidZipf: {
      // A user's rows, interleaved at random with the others', carry its
      // primary version 95 times in 100, else any version.
      const auto& users = uidUsers(spec.partitions);
      std::vector<int32_t> partition(kRowsPerBatch);
      std::vector<int64_t> user(kRowsPerBatch);
      std::vector<uint16_t> version(kRowsPerBatch);
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        const auto [p, k] = users.ids.sample(rng);
        partition[i] = p;
        user[i] = users.ids.ids[p][k];
        version[i] = rng() % 100 < 95 ? users.primary[p][k] : versionIndex(rng);
      }
      return makeRow(
          {"pid", "user_id", "version_id"},
          {partitionColumn(rng, partition),
           bigints(rng, [&](vector_size_t i) { return user[i]; }),
           strings(0, rng, [&](vector_size_t i, std::string& out) {
             out = users.versions[version[i]];
           })});
    }
    case Family::kSeqHash: {
      // Ids of an auto-increment sequence, gap 1, hashed to partitions.
      std::vector<int32_t> partition(kRowsPerBatch);
      std::vector<int64_t> id(kRowsPerBatch);
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        id[i] = kFirstSequenceId + int64_t{batch} * kRowsPerBatch + i;
        partition[i] = hashPartition(id[i], spec.partitions);
      }
      return makeRow(
          {"pid", "id"},
          {partitionColumn(rng, partition),
           bigints(rng, [&](vector_size_t i) { return id[i]; })});
    }
    case Family::kTsEvent: {
      // Event times in milliseconds: rows arrive in order, three a
      // millisecond, each event up to five minutes either side of its
      // arrival.
      auto ts = bigints(rng, [&](vector_size_t i) {
        const int64_t row = int64_t{batch} * kRowsPerBatch + i;
        return kFirstArrivalMs + row / 3 +
            static_cast<int64_t>(rng() % 600'000) - 300'000;
      });
      return makeRow({"pid", "ts"}, {pid, std::move(ts)});
    }
    case Family::kWideRandom:
      return makeRow(
          {"pid", "value"},
          {pid, bigints(rng, [&](vector_size_t i) {
             return wideRandomValue(int64_t{batch} * kRowsPerBatch + i);
           })});
    case Family::kClusteredIds:
    case Family::kIds33To40Bit: {
      const auto& ids = spec.family == Family::kClusteredIds
          ? clusteredIds(spec.partitions)
          : ids33To40Bit(spec.partitions);
      std::vector<int32_t> partition(kRowsPerBatch);
      std::vector<int64_t> id(kRowsPerBatch);
      for (vector_size_t i = 0; i < kRowsPerBatch; ++i) {
        const auto [p, k] = ids.sample(rng);
        partition[i] = p;
        id[i] = ids.ids[p][k];
      }
      return makeRow(
          {"pid", "id"},
          {partitionColumn(rng, partition),
           bigints(rng, [&](vector_size_t i) { return id[i]; })});
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
  const auto& names = batch->type()->asRow().names();
  const auto& children = batch->children();
  return structOf(
      std::vector<std::string>(names.begin() + 1, names.end()),
      std::vector<VectorPtr>(children.begin() + 1, children.end()),
      nullptr,
      batch->size());
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
/// Cell that never seals a window for its null memory; CellAdapter is Cell
/// with nested columns packed by the type adapter instead of shredded.
enum class Writer {
  kCell,
  kCellNoDict,
  kV1,
  kV2,
  kCellUnsealedNulls,
  kCellAdapter,
};

struct WriterInfo {
  Writer writer;
  const char* name;
  ShuffleWriterType type;
};

constexpr WriterInfo kWriters[] = {
    {Writer::kCell, "Cell", ShuffleWriterType::Cell},
    {Writer::kCellNoDict, "CellNoDict", ShuffleWriterType::Cell},
    {Writer::kV1, "V1", ShuffleWriterType::V1},
    {Writer::kV2, "V2", ShuffleWriterType::V2},
    {Writer::kCellUnsealedNulls, "CellUnsealedNulls", ShuffleWriterType::Cell},
    {Writer::kCellAdapter, "CellAdapter", ShuffleWriterType::Cell},
};

const WriterInfo& writerInfo(Writer writer) {
  for (const auto& info : kWriters) {
    if (info.writer == writer) {
      return info;
    }
  }
  BOLT_UNREACHABLE();
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
  options.forceShuffleWriterType =
      static_cast<int32_t>(writerInfo(writer).type);
  options.partitionWriterOptions.numPartitions = c.spec.partitions;
  options.partitionWriterOptions.dataFile = dataFile;
  options.partitionWriterOptions.configuredDirs = {spillDir()};
  options.partitionWriterOptions.numSubDirs = 1;
  options.partitionWriterOptions.compressionType = benchmarkCodec();
  options.partitionWriterOptions.compressionLevel = benchmarkCodecLevel();
  options.cellOptions.enableStringDictionary = writer != Writer::kCellNoDict;
  // Nested columns packed into the adapter's binary column, as before
  // nested layouts.
  options.cellOptions.nativeNestedTypes = writer != Writer::kCellAdapter;
  if (writer == Writer::kCellUnsealedNulls) {
    // Neither the null memory nor a reclaim's share of it closes a window.
    options.cellOptions.nullMemLimitBytes = std::numeric_limits<int64_t>::max();
  }
  options.cellOptions.streamCompression =
      FLAGS_bolt_benchmark_shuffle_cell_stream_compression;
  options.cellOptions.adaptiveCompression =
      FLAGS_bolt_benchmark_shuffle_cell_adaptive_compression;
  options.cellOptions.spillMemoryBytes =
      FLAGS_bolt_benchmark_shuffle_cell_spill_memory_bytes;
  options.cellOptions.cellSpillBytesPerStream =
      FLAGS_bolt_benchmark_shuffle_cell_spill_bytes_per_stream;
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
    // Partitions lie back to back: the skipped ones are empty, so offset_
    // is where partition next_ starts.
    const int64_t length = (*lengths_)[next_++];
    auto stream = std::make_shared<arrow::io::BufferReader>(
        reinterpret_cast<const uint8_t*>(file_->data()) + offset_, length);
    offset_ += length;
    return stream;
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
  int64_t offset_{0};
};

/// Delegating codec that meters decompression, which the Cell reader
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
  if (writerInfo(output.writer).type == ShuffleWriterType::Cell) {
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
        /*batchByteSize=*/1 << 20,
        /*nativeNestedTypes=*/output.writer != Writer::kCellAdapter);
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
    factory.setShuffleWriterType(
        static_cast<int32_t>(writerInfo(output.writer).type));
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
  // The Cell writer's seals, spills and pushes inside its splits; it holds
  // the compression and writes compress_ns and evict_ns count, so other_ns
  // does not take it off.
  setCounter(counters, "seal_spill_ns", m.sealSpillTime, time);
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
  setCounter(counters, "used_cell", m.useCellShuffle);
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
/// once, against the input; after a failed write none is kept.
void keepOutput(const Case& c, Writer writer, const Written& w) {
  if (!w.ok) {
    residentOutput().reset();
    return;
  }
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
      LOG(ERROR) << c.spec.name() << "/" << writerInfo(writer).name
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
  // The spill files PartialSpiller names <dataFile>.<name>.spill.
  ::unlink((dataFile + ".cell.spill").c_str());
  ::unlink((dataFile + ".blocks.spill").c_str());
}

void registerCase(const CaseSpec& spec, const std::vector<Writer>& writers) {
  for (const auto writer : writers) {
    if (writer == Writer::kCellNoDict && !hasTrait(spec.family, kDictionary)) {
      continue;
    }
    if (writer == Writer::kCellAdapter && !hasTrait(spec.family, kNested)) {
      continue;
    }
    if (writer == Writer::kCellUnsealedNulls && !hasHeavyNulls(spec)) {
      continue;
    }
    const std::string prefix = spec.name() + "/" + writerInfo(writer).name;
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
            if (!w.ok) {
              LOG(ERROR) << spec.name() << "/" << writerInfo(writer).name
                         << " failed: " << w.error;
            }
            keepOutput(*c, writer, w);
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
              keepOutput(*c, writer, w);
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
    writers.push_back(Writer::kCellAdapter);
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
          "%-16s %-24s P=%-6d nulls=%-3d input=%lldMB%s%s%s%s\n",
          spec.id.c_str(),
          familyInfo(spec.family).name,
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
              : "",
          spec.hotPartitions > 0
              ? (" hot=" + std::to_string(spec.hotPartitions)).c_str()
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
