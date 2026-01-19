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
#include <folly/init/Init.h>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include "bolt/common/memory/ByteStream.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/serializers/ArrowSerializer.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorStream.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DictionaryVector.h"
#include "bolt/vector/VectorEncoding.h"
#include "bolt/vector/VectorTypeUtils.h"
#include "bolt/vector/arrow/Bridge.h"

using ArrowSerde = bytedance::bolt::serializer::arrowserde::ArrowVectorSerde;

using namespace bytedance::bolt;
using namespace bytedance::bolt::serializer;
using namespace bytedance::bolt::test;

DEFINE_string(codec, "none", "Compression codec to use in the benchmark.");

namespace {

using Clock = std::chrono::steady_clock;
inline uint64_t nowUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             Clock::now().time_since_epoch())
      .count();
}

struct Stat {
  uint64_t serUs{0};
  uint64_t deUs{0};
  size_t bytes{0};
};

struct OneResult {
  std::string note;
  std::string schema;
  vector_size_t rows{0};
  int nullPct{0};
  size_t nullCount{0};
  double avgLen{-1.0};
  std::string headRaw;
  std::string headNN; // first few non-null
  std::string boltPhys;
  std::string arrowSchemaDictAware;

  Stat presto;
  Stat arrow;
};

static std::string physOf(const BaseVector& v);

static std::string physChild(const BaseVector* child) {
  return child ? physOf(*child->loadedVector()) : "<null>";
}

static std::string physOf(const BaseVector& v) {
  auto enc = v.encoding();
  switch (enc) {
    case VectorEncoding::Simple::FLAT: {
      return std::string("FLAT[") + v.type()->toString() + "]";
    }
    case VectorEncoding::Simple::DICTIONARY: {
      const BaseVector* base = v.wrappedVector();
      size_t card = base ? base->size() : 0;
      return std::string("DICTIONARY[index=i32,card=") + std::to_string(card) +
          "](" + physChild(base) + ")";
    }
    case VectorEncoding::Simple::CONSTANT: {
      return std::string("CONSTANT(") + physChild(v.wrappedVector()) + ")";
    }
    case VectorEncoding::Simple::ROW: {
      const auto& row = *v.asUnchecked<RowVector>();
      std::string s = "ROW{";
      for (size_t i = 0; i < row.childrenSize(); ++i) {
        if (i)
          s += ", ";
        s += physChild(row.childAt(i).get());
      }
      s += "}";
      return s;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* a = v.asUnchecked<ArrayVector>();
      return std::string("ARRAY[") + physChild(a->elements().get()) + "]";
    }
    case VectorEncoding::Simple::MAP: {
      auto* m = v.asUnchecked<MapVector>();
      return std::string("MAP<") + physChild(m->mapKeys().get()) + "," +
          physChild(m->mapValues().get()) + ">";
    }
    default:
      return std::string("ENC(") + mapSimpleToName(enc) + ")";
  }
}

static std::string buildBoltPhys(const RowVectorPtr& rv) {
  std::ostringstream os;
  os << "[bolt][phys] " << physOf(*rv);
  if (rv->childrenSize() > 0) {
    os << "\n  - c0: " << physOf(*rv->childAt(0));
  }
  return os.str();
}

static std::string buildArrowSchemaDictAware(const RowVectorPtr& rv) {
  ArrowOptions opts;
  opts.flattenDictionary = false;
  opts.flattenConstant = false;

  ArrowSchema cSchema{};
  std::string out;
  try {
    exportToArrow(rv, cSchema, opts, {});
    auto schRes = ::arrow::ImportSchema(&cSchema);
    if (schRes.ok()) {
      out = std::string("[arrow][schema][flattenDict=false] ") +
          schRes.ValueUnsafe()->ToString();
    } else {
      out = std::string("[arrow][schema] ImportSchema failed: ") +
          schRes.status().ToString();
    }
  } catch (const std::exception& e) {
    out = std::string("[arrow][schema] exception: ") + e.what();
  }
  if (cSchema.release)
    cSchema.release(&cSchema);
  return out;
}

class Bench : public VectorTestBase {
  common::CompressionKind kind_;

 public:
  Bench(const std::string& codec) {
    kind_ = common::stringToCompressionKind(codec);
  };

  static std::string headFirstK(const RowVectorPtr& row, int k = 3) {
    std::ostringstream oss;
    auto v = row->childAt(0);
    auto n = std::min<vector_size_t>(row->size(), k);
    for (vector_size_t i = 0; i < n; ++i) {
      oss << "[i=" << i << "] ";
      if (v->isNullAt(i)) {
        oss << "NULL ";
        continue;
      }
      printOneValue(oss, v, i);
      oss << " ";
    }
    return oss.str();
  }

  static std::string headFirstKNonNull(const RowVectorPtr& row, int k = 3) {
    std::ostringstream oss;
    auto v = row->childAt(0);
    int found = 0;
    for (vector_size_t i = 0; i < row->size() && found < k; ++i) {
      if (v->isNullAt(i))
        continue;
      oss << "[i=" << i << "] ";
      printOneValue(oss, v, i);
      oss << " ";
      ++found;
    }
    if (found == 0)
      return "<no non-null in first N>";
    return oss.str();
  }

  static void
  printOneValue(std::ostream& os, const VectorPtr& v, vector_size_t i) {
    switch (v->typeKind()) {
      case TypeKind::BOOLEAN: {
        bool val = v->as<SimpleVector<bool>>()->valueAt(i);
        os << (val ? "true" : "false");
        break;
      }
      case TypeKind::TINYINT:
        os << static_cast<int>(v->as<SimpleVector<int8_t>>()->valueAt(i));
        break;
      case TypeKind::SMALLINT:
        os << v->as<SimpleVector<int16_t>>()->valueAt(i);
        break;
      case TypeKind::INTEGER:
        os << v->as<SimpleVector<int32_t>>()->valueAt(i);
        break;
      case TypeKind::BIGINT:
        os << v->as<SimpleVector<int64_t>>()->valueAt(i);
        break;
      case TypeKind::REAL:
        os << v->as<SimpleVector<float>>()->valueAt(i);
        break;
      case TypeKind::DOUBLE:
        os << v->as<SimpleVector<double>>()->valueAt(i);
        break;
      case TypeKind::VARCHAR: {
        auto sv = v->as<SimpleVector<StringView>>()->valueAt(i);
        size_t show = std::min<size_t>(sv.size(), 12);
        os << '"' << std::string(sv.data(), show) << (sv.size() > 12 ? "…" : "")
           << "\"(" << sv.size() << ")";
        break;
      }
      default:
        os << "<" << v->type()->toString() << ">";
    }
  }

  static size_t measuredNullsC0(const RowVectorPtr& row) {
    auto v = row->childAt(0);
    if (!v->mayHaveNulls())
      return 0;
    return BaseVector::countNulls(v->nulls(), 0, row->size());
  }

  static double measuredAvgLenIfVarcharC0(const RowVectorPtr& row) {
    auto v = row->childAt(0);
    if (v->typeKind() != TypeKind::VARCHAR)
      return -1.0;
    const auto sv = v->as<SimpleVector<StringView>>();
    const vector_size_t n = row->size();
    const vector_size_t sampleN = std::min<vector_size_t>(n, 4096);
    if (sampleN == 0)
      return 0.0;
    size_t cnt = 0, sum = 0;
    for (vector_size_t i = 0; i < sampleN; ++i) {
      if (sv->isNullAt(i))
        continue;
      sum += sv->valueAt(i).size();
      ++cnt;
    }
    return cnt ? static_cast<double>(sum) / static_cast<double>(cnt) : 0.0;
  }

  VectorPtr
  makeFlatVectorByType(const TypePtr& t, vector_size_t n, int nullPct) {
    VectorMaker vm(pool_.get());
    auto nullAt = [&](vector_size_t row) -> bool {
      return nullPct == 0 ? false : (row % 100) < nullPct;
    };

    switch (t->kind()) {
      case TypeKind::BOOLEAN:
        return vm.flatVector<bool>(
            n, [&](auto row) { return (row & 1) == 0; }, nullAt);
      case TypeKind::TINYINT:
        return vm.flatVector<int8_t>(
            n,
            [&](auto row) { return static_cast<int8_t>(row % 127); },
            nullAt);
      case TypeKind::SMALLINT:
        return vm.flatVector<int16_t>(
            n,
            [&](auto row) { return static_cast<int16_t>(row % 1000); },
            nullAt);
      case TypeKind::INTEGER:
        return vm.flatVector<int32_t>(
            n, [&](auto row) { return static_cast<int32_t>(row); }, nullAt);
      case TypeKind::BIGINT:
        return vm.flatVector<int64_t>(
            n, [&](auto row) { return static_cast<int64_t>(row); }, nullAt);
      case TypeKind::REAL:
        return vm.flatVector<float>(
            n,
            [&](auto row) { return static_cast<float>(row) * 0.5f; },
            nullAt);
      case TypeKind::DOUBLE:
        return vm.flatVector<double>(
            n,
            [&](auto row) { return static_cast<double>(row) * 0.1; },
            nullAt);
      case TypeKind::VARCHAR: {
        static const char charset[] =
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "0123456789";
        static constexpr size_t M = sizeof(charset) - 1;
        return vm.flatVector<std::string>(
            n,
            [&](vector_size_t row) {
              std::string s(12, '\0');
              for (size_t i = 0; i < 12; ++i) {
                s[i] = charset[(static_cast<size_t>(row) + i * 31) % M];
              }
              return s;
            },
            nullAt);
      }
      default:
        BOLT_FAIL("Unsupported scalar type: {}", t->toString());
    }
  }

  RowVectorPtr
  makeRowVectorForCase(const TypePtr& elem, vector_size_t n, int nullPct) {
    VectorPtr child = makeFlatVectorByType(elem, n, nullPct);
    return makeRowVector({child});
  }

  RowVectorPtr
  makeRowVectorFixedLenVarchar(vector_size_t n, int nullPct, size_t len) {
    VectorMaker vm(pool_.get());
    auto nullAt = [&](vector_size_t row) -> bool {
      return nullPct == 0 ? false : (row % 100) < nullPct;
    };
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";
    static constexpr size_t M = sizeof(charset) - 1;

    auto child = vm.flatVector<std::string>(
        n,
        [&](vector_size_t row) {
          std::string s(len, '\0');
          for (size_t i = 0; i < len; ++i) {
            s[i] = charset[(static_cast<size_t>(row) + i * 31) % M];
          }
          return s;
        },
        nullAt);
    return makeRowVector({child});
  }

  RowVectorPtr
  makeDictI64(vector_size_t n, int nullPct, vector_size_t cardinality) {
    auto base = makeFlatVector<int64_t>(
        cardinality, [](auto i) { return static_cast<int64_t>(i); });
    BufferPtr nulls = AlignedBuffer::allocate<bool>(n, pool_.get());
    auto rawNulls = nulls->asMutable<uint64_t>();
    for (vector_size_t i = 0; i < n; ++i) {
      bits::setNull(rawNulls, i, (nullPct != 0) && ((i % 100) < nullPct));
    }
    BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(n, pool_.get());
    auto rawIdx = indices->asMutable<vector_size_t>();
    for (vector_size_t i = 0; i < n; ++i) {
      if (!((nullPct != 0) && ((i % 100) < nullPct))) {
        rawIdx[i] = i % cardinality;
      }
    }
    auto dict = BaseVector::wrapInDictionary(nulls, indices, n, base);
    return makeRowVector({dict});
  }

  RowVectorPtr makeDictVarchar(
      vector_size_t n,
      int nullPct,
      vector_size_t cardinality,
      size_t len) {
    VectorMaker vm(pool_.get());
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";
    static constexpr size_t M = sizeof(charset) - 1;

    auto base = vm.flatVector<std::string>(cardinality, [&](vector_size_t row) {
      std::string s(len, '\0');
      for (size_t i = 0; i < len; ++i) {
        s[i] = charset[(static_cast<size_t>(row) + i * 31) % M];
      }
      return s;
    });

    BufferPtr nulls = AlignedBuffer::allocate<bool>(n, pool_.get());
    auto rawNulls = nulls->asMutable<uint64_t>();
    for (vector_size_t i = 0; i < n; ++i) {
      bits::setNull(rawNulls, i, (nullPct != 0) && ((i % 100) < nullPct));
    }
    BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(n, pool_.get());
    auto rawIdx = indices->asMutable<vector_size_t>();
    for (vector_size_t i = 0; i < n; ++i) {
      if (!((nullPct != 0) && ((i % 100) < nullPct))) {
        rawIdx[i] = i % cardinality;
      }
    }
    auto dict = BaseVector::wrapInDictionary(nulls, indices, n, base);
    return makeRowVector({dict});
  }

  Stat runOnceArrowIOBuf(const RowTypePtr& rowType, const RowVectorPtr& input) {
    ArrowSerde arrowSerde;
    Stat st{};
    auto t0 = nowUs();
    ArrowSerde::ArrowSerdeOptions options(false, kind_);
    auto iobuf = rowVectorToIOBuf(input, *pool_, &arrowSerde, &options);
    st.serUs = nowUs() - t0;

    st.bytes = iobuf.computeChainDataLength();

    t0 = nowUs();
    auto out = IOBufToRowVector(
        std::move(iobuf), rowType, *pool_, &arrowSerde, &options);
    st.deUs = nowUs() - t0;

    BOLT_CHECK_NOT_NULL(out);
    BOLT_CHECK_EQ(out->size(), input->size());
    assertEqualVectors(input, out);
    return st;
  }

  Stat runOncePrestoIOBuf(
      const RowTypePtr& rowType,
      const RowVectorPtr& input) {
    serializer::presto::PrestoVectorSerde prestoSerde;
    Stat st{};
    auto t0 = nowUs();
    serializer::presto::PrestoVectorSerde::PrestoOptions options(false, kind_);
    auto iobuf = rowVectorToIOBuf(input, *pool_, &prestoSerde, &options);
    st.serUs = nowUs() - t0;

    st.bytes = iobuf.computeChainDataLength();

    t0 = nowUs();
    auto out = IOBufToRowVector(
        std::move(iobuf), rowType, *pool_, &prestoSerde, &options);
    st.deUs = nowUs() - t0;

    BOLT_CHECK_NOT_NULL(out);
    BOLT_CHECK_EQ(out->size(), input->size());
    assertEqualVectors(input, out);
    return st;
  }

  template <typename Fn>
  Stat runMulti(Fn&& fn, int iters) {
    (void)fn(); // cache warm up
    Stat best{};
    best.serUs = std::numeric_limits<uint64_t>::max();
    best.deUs = std::numeric_limits<uint64_t>::max();
    best.bytes = 0;
    for (int i = 0; i < iters; ++i) {
      auto st = fn();
      if (st.serUs < best.serUs)
        best.serUs = st.serUs;
      if (st.deUs < best.deUs)
        best.deUs = st.deUs;
      best.bytes = st.bytes;
    }
    return best;
  }

  OneResult runCase(
      const std::string& note,
      const TypePtr& elemType,
      vector_size_t rows,
      int nullPct,
      int iters) {
    OneResult r;
    r.note = note;
    r.rows = rows;
    r.nullPct = nullPct;

    auto row = makeRowVectorForCase(elemType, rows, nullPct);
    r.schema = row->type()->toString();
    r.headRaw = headFirstK(row);
    r.headNN = headFirstKNonNull(row);
    r.nullCount = measuredNullsC0(row);
    r.avgLen = measuredAvgLenIfVarcharC0(row);

    r.boltPhys = buildBoltPhys(row);
    r.arrowSchemaDictAware = buildArrowSchemaDictAware(row);

    auto rowType = asRowType(row->type());
    r.presto =
        runMulti([&] { return runOncePrestoIOBuf(rowType, row); }, iters);
    r.arrow = runMulti([&] { return runOnceArrowIOBuf(rowType, row); }, iters);
    return r;
  }

  OneResult runCaseVector(
      const std::string& note,
      const RowVectorPtr& row,
      int iters,
      int nullPct) {
    OneResult r;
    r.note = note;
    r.schema = row->type()->toString();
    r.rows = row->size();
    r.nullPct = nullPct;
    r.headRaw = headFirstK(row);
    r.headNN = headFirstKNonNull(row);
    r.nullCount = measuredNullsC0(row);
    r.avgLen = measuredAvgLenIfVarcharC0(row);

    r.boltPhys = buildBoltPhys(row);
    r.arrowSchemaDictAware = buildArrowSchemaDictAware(row);

    auto rowType = asRowType(row->type());
    r.presto =
        runMulti([&] { return runOncePrestoIOBuf(rowType, row); }, iters);
    r.arrow = runMulti([&] { return runOnceArrowIOBuf(rowType, row); }, iters);
    return r;
  }

  void printOne(const OneResult& r) {
    const uint64_t pSum = r.presto.serUs + r.presto.deUs;
    const uint64_t aSum = r.arrow.serUs + r.arrow.deUs;

    auto ratio = [&](uint64_t base, uint64_t val) -> std::string {
      if (val == 0)
        return "(n/a)";
      double x = (double)base / (double)val;
      std::ostringstream oss;
      oss << std::fixed << std::setprecision(2) << "(" << x << "x)";
      return oss.str();
    };
    auto pct = [&](size_t base, size_t val) -> std::string {
      if (base == 0)
        return "(n/a)";
      double x = 100.0 * (double)val / (double)base;
      std::ostringstream oss;
      oss << std::fixed << std::setprecision(2) << "(" << x << "%)";
      return oss.str();
    };

    const double effNullPct = r.rows == 0
        ? 0.0
        : (100.0 * static_cast<double>(r.nullCount) /
           static_cast<double>(r.rows));

    std::cout << "case=[" << r.note << "], schema=" << r.schema
              << ", rows=" << r.rows << ", cfg_null%=" << r.nullPct
              << ", eff_null%=" << std::fixed << std::setprecision(2)
              << effNullPct << "\n";

    if (!r.headRaw.empty()) {
      std::cout << "  head(raw)   c0: " << r.headRaw << "\n";
    }
    if (!r.headNN.empty()) {
      std::cout << "  head(non-n) c0: " << r.headNN << "\n";
    }
    if (r.avgLen >= 0.0) {
      std::cout << "  avgLen(c0, sample) ≈ " << std::fixed
                << std::setprecision(2) << r.avgLen << "\n";
    }

    if (!r.boltPhys.empty()) {
      std::cout << "  " << r.boltPhys << "\n";
    }
    if (!r.arrowSchemaDictAware.empty()) {
      std::cout << "  " << r.arrowSchemaDictAware << "\n";
    }

    std::cout << "  Presto (IOBuf): "
              << "ser=" << r.presto.serUs << "  de=" << r.presto.deUs
              << "  sum=" << pSum << "  bytes=" << r.presto.bytes << "\n";

    std::cout << "  Arrow  (IOBuf): "
              << "ser=" << r.arrow.serUs << " "
              << ratio(r.presto.serUs, r.arrow.serUs) << "  de=" << r.arrow.deUs
              << " " << ratio(r.presto.deUs, r.arrow.deUs) << "  sum=" << aSum
              << " " << ratio(pSum, aSum) << "  bytes=" << r.arrow.bytes << " "
              << pct(r.presto.bytes, r.arrow.bytes) << "\n\n";
  }

  void runAll() {
    const int iters = 10;
    const int nullPct = 10;

    const vector_size_t N_BASE = 1'000'000;
    const vector_size_t N_LONG = 65'536;

    struct CaseDef {
      std::string note;
      TypePtr type;
    };
    std::vector<CaseDef> scalars = {
        {"bool", BOOLEAN()},
        {"i8", TINYINT()},
        {"i16", SMALLINT()},
        {"i32", INTEGER()},
        {"i64", BIGINT()},
        {"f32", REAL()},
        {"f64", DOUBLE()},
    };
    for (auto& c : scalars) {
      printOne(runCase(c.note, c.type, N_BASE, nullPct, iters));
    }
    for (auto& c : scalars) {
      printOne(runCase(c.note, c.type, N_BASE, 0, iters));
    }

    {
      auto row = makeRowVectorFixedLenVarchar(N_BASE, nullPct, 12);
      printOne(runCaseVector("str_inline[len=12]", row, iters, nullPct));
    }
    {
      auto row = makeRowVectorFixedLenVarchar(N_LONG, nullPct, 8 * 1024);
      printOne(runCaseVector("str_long[len=8KiB]", row, iters, nullPct));
    }

    const vector_size_t dictCard = 100;
    {
      auto row = makeDictI64(N_BASE, nullPct, dictCard);
      printOne(runCaseVector("dict<i64>[card=100]", row, iters, nullPct));
    }
    {
      auto row = makeDictVarchar(N_BASE, nullPct, dictCard, 12);
      printOne(runCaseVector(
          "dict<str_inline>[len=12,card=100]", row, iters, nullPct));
    }
    {
      auto row = makeDictVarchar(N_LONG, nullPct, dictCard, 8 * 1024);
      printOne(runCaseVector(
          "dict<str_long>[len=8KiB,card=100]", row, iters, nullPct));
    }
    {
      auto row = makeDictVarchar(N_LONG, nullPct, dictCard * 100, 8 * 1024);
      printOne(runCaseVector(
          "dict<str_long>[len=8KiB,card=100*100]", row, iters, nullPct));
    }

    // =========================================================================
    // Option 1.5: Dictionary-Wrapped RowVector (HashJoin Partition Simulation)
    // =========================================================================
    std::cout << "\n";
    std::cout
        << "=============================================================\n";
    std::cout
        << "=== Dictionary-Wrapped RowVector (HashJoin Sim)           ===\n";
    std::cout
        << "=============================================================\n";

    auto runDictWrappedCase = [&](const std::string& name,
                                  const RowVectorPtr& base,
                                  std::vector<vector_size_t> sizes) {
      std::cout << "\n--- " << name << " (wrapped) ---\n";
      std::cout
          << "rows\tArrow_ser\tArrow_de\tus/row\t\tPresto_ser\tPresto_de\tratio\n";

      for (auto bs : sizes) {
        // Create indices for wrapping
        BufferPtr indices =
            AlignedBuffer::allocate<vector_size_t>(bs, pool_.get());
        auto rawIdx = indices->asMutable<vector_size_t>();
        for (vector_size_t i = 0; i < bs; ++i) {
          rawIdx[i] = i % base->size();
        }

        std::vector<VectorPtr> wrappedChildren;
        for (size_t i = 0; i < base->childrenSize(); ++i) {
          wrappedChildren.push_back(BaseVector::wrapInDictionary(
              nullptr, indices, bs, base->childAt(i)));
        }
        auto wrappedRow = std::make_shared<RowVector>(
            pool_.get(), base->type(), nullptr, bs, wrappedChildren);

        auto r = runCaseVector(name, wrappedRow, iters, nullPct);

        double usPerRow =
            bs > 0 ? (double)(r.arrow.serUs + r.arrow.deUs) / (double)bs : 0.0;
        double ratio = r.presto.serUs > 0
            ? (double)r.arrow.serUs / (double)r.presto.serUs
            : 0.0;
        std::cout << bs << "\t" << r.arrow.serUs << "\t\t" << r.arrow.deUs
                  << "\t\t" << std::fixed << std::setprecision(3) << usPerRow
                  << "\t\t" << r.presto.serUs << "\t\t" << r.presto.deUs
                  << "\t\t" << std::setprecision(2) << ratio << "x\n";
      }
    };

    std::vector<vector_size_t> testSizes = {
        1,
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024,
        2048,
        4096,
        8192,
        16384,
        32768};

    // Case 1: Wrapped i64
    {
      auto base = makeRowVectorForCase(BIGINT(), 32768, nullPct);
      runDictWrappedCase("i64", base, testSizes);
    }

    // Case 2: Wrapped Varchar
    {
      auto base = makeRowVectorFixedLenVarchar(32768, nullPct, 12);
      runDictWrappedCase("varchar[12]", base, testSizes);
    }

    // =========================================================================
    // Option 1.6: Fixed Overhead Analysis (Small Batch)
    // Use accumulated timing for small batches to get accurate measurements
    // =========================================================================
    std::cout << "\n";
    std::cout
        << "=============================================================\n";
    std::cout
        << "=== Fixed Overhead Analysis (Small Batch)                 ===\n";
    std::cout
        << "=============================================================\n";
    std::cout << "rows\tArrow_ser(us)\tPresto_ser(us)\tratio\n";

    std::vector<vector_size_t> smallSizes = {
        1,
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024,
        2048,
        4096,
        8192,
        16384,
        32768};

    for (auto bs : smallSizes) {
      // Adjust iterations based on batch size to keep test time reasonable
      int accumulateRuns = bs <= 256 ? 1000 : (bs <= 4096 ? 200 : 50);

      auto row = makeRowVectorForCase(BIGINT(), bs, nullPct);
      auto rowType = asRowType(row->type());

      // Measure Arrow accumulated time
      uint64_t arrowTotalUs = 0;
      {
        auto t0 = nowUs();
        for (int run = 0; run < accumulateRuns; ++run) {
          ArrowSerde arrowSerde;
          ArrowSerde::ArrowSerdeOptions options(false, kind_);
          auto iobuf = rowVectorToIOBuf(row, *pool_, &arrowSerde, &options);
          (void)iobuf.computeChainDataLength();
        }
        arrowTotalUs = nowUs() - t0;
      }

      // Measure Presto accumulated time
      uint64_t prestoTotalUs = 0;
      {
        auto t0 = nowUs();
        for (int run = 0; run < accumulateRuns; ++run) {
          serializer::presto::PrestoVectorSerde prestoSerde;
          serializer::presto::PrestoVectorSerde::PrestoOptions options(
              false, kind_);
          auto iobuf = rowVectorToIOBuf(row, *pool_, &prestoSerde, &options);
          (void)iobuf.computeChainDataLength();
        }
        prestoTotalUs = nowUs() - t0;
      }

      double arrowAvgUs = (double)arrowTotalUs / accumulateRuns;
      double prestoAvgUs = (double)prestoTotalUs / accumulateRuns;
      double ratio = prestoAvgUs > 0.001 ? arrowAvgUs / prestoAvgUs : 0.0;

      std::cout << bs << "\t" << std::fixed << std::setprecision(1)
                << arrowAvgUs << "\t\t" << prestoAvgUs << "\t\t"
                << std::setprecision(2) << ratio << "x\n";
    }
  }
};

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  bytedance::bolt::memory::initializeMemoryManager({});
  try {
    Bench bench(FLAGS_codec);
    bench.runAll();
  } catch (const std::exception& e) {
    std::cerr << "Benchmark failed: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
