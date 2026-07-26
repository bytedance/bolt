#pragma once

#ifdef ENABLE_BOLT_JIT

#include <cstdint>
#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Module.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/jit/CompiledModule.h"
#include "bolt/jit/aggregation/HashAggrJitTypes.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::jit {

class HashAggrJitCodegen;
class InputAdapterCodegen;
class OutputAdapterCodegen;
struct HashAggrJitExtractTarget;

struct HashAggrJitOps {
  using CreateFn =
      void (*)(HashAggrJitCodegen&, llvm::Value* group, const HashAggrJitSlot&);
  using AddFn = void (*)(
      HashAggrJitCodegen&,
      llvm::Value* group,
      const InputAdapterCodegen& input,
      llvm::Value* row,
      const HashAggrJitSlot&,
      llvm::BasicBlock* nextBlock);
  using ExtractFn = void (*)(
      HashAggrJitCodegen&,
      llvm::Value* group,
      const HashAggrJitSlot&,
      const HashAggrJitExtractTarget&);

  CreateFn initGroup;
  AddFn addRawInput;
  AddFn addIntermediateResults;
  // Writes the intermediate (partial) accumulator state to the output, mirroring
  // the non-JIT extractAccumulators path.
  ExtractFn extractAccumulators;
  // Writes the final aggregate result to the output, mirroring the non-JIT
  // extractValues/extractResults path.
  ExtractFn extractResults;
};

struct HashAggrJitExtractTarget {
  // Codegen adapter for the destination output vector to write results into.
  const OutputAdapterCodegen& output;
  // The target row index (runtime llvm::Value) to write the extracted result.
  llvm::Value* row;
};

class IRRow {
 public:
  // Framework-level invariant: IRRow<T> = {T, i1}. 'valueType' is owned by
  // aggregate semantics; the null bit is always field 1.
  static llvm::StructType* getType(
      llvm::IRBuilder<>& builder,
      llvm::Type* valueType) {
    return llvm::StructType::get(valueType, builder.getInt1Ty());
  }

  static llvm::Value* getValue(llvm::IRBuilder<>& builder, llvm::Value* row) {
    return builder.CreateExtractValue(row, {0});
  }

  static llvm::Value* getIsNull(llvm::IRBuilder<>& builder, llvm::Value* row) {
    return builder.CreateExtractValue(row, {1});
  }

  static llvm::Value*
  pack(llvm::IRBuilder<>& builder, llvm::Value* value, llvm::Value* isNull) {
    auto* rowType = getType(builder, value->getType());
    auto* withValue =
        builder.CreateInsertValue(llvm::UndefValue::get(rowType), value, {0});
    return builder.CreateInsertValue(withValue, isNull, {1});
  }

  static llvm::Value*
  withValue(llvm::IRBuilder<>& builder, llvm::Value* row, llvm::Value* value) {
    return builder.CreateInsertValue(row, value, {0});
  }

  static llvm::Value* withIsNull(
      llvm::IRBuilder<>& builder,
      llvm::Value* row,
      llvm::Value* isNull) {
    return builder.CreateInsertValue(row, isNull, {1});
  }

  // Nested value access for aggregate-owned composite payloads, e.g.
  // IRRow<{double, i64}> = {{double, i64}, i1}.
  static llvm::Value*
  getValueField(llvm::IRBuilder<>& builder, llvm::Value* row, unsigned field) {
    return builder.CreateExtractValue(row, {0, field});
  }
};

class InputAdapterCodegen {
 public:
  virtual ~InputAdapterCodegen() = default;

  virtual llvm::StructType* irRowType(HashAggrJitValueKind kind) const = 0;
  virtual llvm::Value* read(llvm::Value* row, HashAggrJitValueKind kind)
      const = 0;
  virtual llvm::Value* loadNulls() const = 0;
  virtual llvm::Value* isNull(llvm::Value* row) const = 0;
  virtual llvm::Value* readRowField(
      llvm::Value* row,
      int32_t field,
      HashAggrJitValueKind kind) const = 0;
  // Reads only the raw value of a ROW child, skipping the per-field null check
  // CFG. Use when the framework guarantees the field is non-null on this path
  // (i.e. the field's null bit is not consumed by the aggregate semantics).
  virtual llvm::Value* readRowFieldValue(
      llvm::Value* row,
      int32_t field,
      HashAggrJitValueKind kind) const = 0;
};

class ScalarInputAdapterCodegen final : public InputAdapterCodegen {
 public:
  ScalarInputAdapterCodegen(HashAggrJitCodegen& codegen, llvm::Value* input);
  ScalarInputAdapterCodegen(
      HashAggrJitCodegen& codegen,
      llvm::Value* input,
      bool useIdentityMapping);

  llvm::StructType* irRowType(HashAggrJitValueKind kind) const override;
  llvm::Value* read(llvm::Value* row, HashAggrJitValueKind kind) const override;
  llvm::Value* loadNulls() const override;
  llvm::Value* isNull(llvm::Value* row) const override;
  llvm::Value* readRowField(llvm::Value*, int32_t, HashAggrJitValueKind)
      const override;
  llvm::Value* readRowFieldValue(llvm::Value*, int32_t, HashAggrJitValueKind)
      const override;

 private:
  HashAggrJitCodegen& codegen_;
  llvm::Value* input_;
  bool useIdentityMapping_{false};
};

class RowInputAdapterCodegen final : public InputAdapterCodegen {
 public:
  RowInputAdapterCodegen(HashAggrJitCodegen& codegen, llvm::Value* input);
  RowInputAdapterCodegen(
      HashAggrJitCodegen& codegen,
      llvm::Value* input,
      bool useIdentityMapping);

  llvm::StructType* irRowType(HashAggrJitValueKind kind) const override;
  llvm::Value* read(llvm::Value*, HashAggrJitValueKind) const override;
  llvm::Value* loadNulls() const override;
  llvm::Value* isNull(llvm::Value* row) const override;
  llvm::Value* readRowField(
      llvm::Value* row,
      int32_t field,
      HashAggrJitValueKind kind) const override;
  llvm::Value* readRowFieldValue(
      llvm::Value* row,
      int32_t field,
      HashAggrJitValueKind kind) const override;

 private:
  llvm::Value* loadChild(int32_t field) const;
  llvm::Value* isRowFieldNull(llvm::Value* row, int32_t field) const;

  HashAggrJitCodegen& codegen_;
  llvm::Value* input_;
  bool useIdentityMapping_{false};
};

class OutputAdapterCodegen {
 public:
  virtual ~OutputAdapterCodegen() = default;

  virtual llvm::Value* vector() const = 0;
  virtual void resize(llvm::Value* size) const = 0;
  virtual void write(
      llvm::Value* row,
      HashAggrJitValueKind kind,
      llvm::Value* irRow) const = 0;
  virtual void writeField(
      llvm::Value* row,
      int32_t field,
      HashAggrJitValueKind kind,
      llvm::Value* irRow) const = 0;
  virtual void writeNull(llvm::Value* row, llvm::Value* isNull) const = 0;
};

class ScalarOutputAdapterCodegen final : public OutputAdapterCodegen {
 public:
  ScalarOutputAdapterCodegen(HashAggrJitCodegen& codegen, llvm::Value* output);

  llvm::Value* vector() const override;
  void resize(llvm::Value* size) const override;
  void write(
      llvm::Value* row,
      HashAggrJitValueKind kind,
      llvm::Value* irRow) const override;
  void writeField(llvm::Value*, int32_t, HashAggrJitValueKind, llvm::Value*)
      const override;
  void writeNull(llvm::Value* row, llvm::Value* isNull) const override;

 private:
  HashAggrJitCodegen& codegen_;
  llvm::Value* output_;
};

class RowOutputAdapterCodegen final : public OutputAdapterCodegen {
 public:
  RowOutputAdapterCodegen(HashAggrJitCodegen& codegen, llvm::Value* output);

  llvm::Value* vector() const override;
  void resize(llvm::Value* size) const override;
  void write(llvm::Value*, HashAggrJitValueKind, llvm::Value*) const override;
  void writeField(
      llvm::Value* row,
      int32_t field,
      HashAggrJitValueKind kind,
      llvm::Value* irRow) const override;
  void writeNull(llvm::Value* row, llvm::Value* isNull) const override;

 private:
  llvm::Value* loadChild(int32_t field) const;

  HashAggrJitCodegen& codegen_;
  llvm::Value* output_;
};

class HashAggrJitCodegen {
 public:
  explicit HashAggrJitCodegen(llvm::Module& module);

  llvm::Module& module() const {
    return module_;
  }

  llvm::IRBuilder<>& builder() const {
    return *builder_;
  }

  void setBuilder(llvm::IRBuilder<>* builder) {
    builder_ = builder;
  }

  llvm::Type* llvmType(HashAggrJitValueKind kind) const;
  llvm::Value* isInputNull(llvm::Value* nulls, llvm::Value* row) const;
  llvm::Value* isAccumulatorNull(
      llvm::Value* group,
      const HashAggrJitSlot& slot) const;
  void clearAccumulatorNull(llvm::Value* group, const HashAggrJitSlot& slot)
      const;
  void clearAccumulatorNullIfNeeded(
      llvm::Value* group,
      const HashAggrJitSlot& slot) const;
  void setAccumulatorNull(llvm::Value* group, const HashAggrJitSlot& slot)
      const;
  bool skipAccumulatorNullClear() const {
    return skipAccumulatorNullClear_;
  }
  void setSkipAccumulatorNullClear(bool skip) {
    skipAccumulatorNullClear_ = skip;
  }
  llvm::LoadInst* loadValue(llvm::Value* row, llvm::Type* type, int32_t offset)
      const;
  void storeValue(
      llvm::Value* row,
      llvm::Type* type,
      int32_t offset,
      llvm::Value* value) const;
  llvm::Value* castValue(
      llvm::Value* value,
      HashAggrJitValueKind from,
      HashAggrJitValueKind to) const;
  bool isFloatKind(HashAggrJitValueKind kind) const;

 private:
  llvm::Module& module_;
  llvm::IRBuilder<>* builder_{nullptr};
  bool skipAccumulatorNullClear_{false};
};

using HashAggrJitAddDenseFunc =
    void (*)(char** groups, int32_t numRows, char** inputRuntimes);
using HashAggrJitInitFunc = void (*)(char** newGroups, int32_t numNewGroups);
using HashAggrJitExtractFunc = void (*)(char** groups, int32_t numGroups, char** resultVectors);

class HashAggrJitChunk {
 public:
  explicit HashAggrJitChunk(
      std::vector<HashAggrJitSlot> slots,
      bool compileExtract = true);

  bool codegen(uint64_t* codegenTimeNs = nullptr);

  bool isCodegenReady() const {
    return ready_.load(std::memory_order_acquire);
  }

  void init(char** newGroups, int32_t numNewGroups) const {
    init_(newGroups, numNewGroups);
  }

  void addDense(
      char** groups,
      int32_t numRows,
      char** inputRuntimes,
      bool inputsMayHaveNulls) const {
    if (!inputsMayHaveNulls && addDenseNoNull_ != nullptr) {
      checkNoRuntimeNulls(inputRuntimes);
      if (addDenseNoNullIdentity_ != nullptr &&
          allInputsUseIdentityMapping(inputRuntimes)) {
        addDenseNoNullIdentity_(groups, numRows, inputRuntimes);
        return;
      }
      addDenseNoNull_(groups, numRows, inputRuntimes);
      return;
    }
    addDense_(groups, numRows, inputRuntimes);
  }

  void extract(char** groups, int32_t numGroups, char** resultVectors) const {
    BOLT_CHECK_NOT_NULL(extract_);
    extract_(groups, numGroups, resultVectors);
  }

  const std::vector<HashAggrJitSlot>& slots() const {
    return slots_;
  }

  std::string getDescription() const;

  const std::string& functionName() const {
    return functionName_;
  }

 private:
  void checkNoRuntimeNulls(char** inputRuntimes) const {
    for (auto i = 0; i < slots_.size(); ++i) {
      const auto& slot = slots_[i];
      if (slot.desc.isCountStar()) {
        continue;
      }
      auto* input =
          reinterpret_cast<const HashAggrJitInputRuntime*>(inputRuntimes[i]);
      if (slot.desc.inputShape() == HashAggrJitRuntimeShape::Row) {
        BOLT_CHECK(
            input->row.nulls == nullptr,
            "HashAggrJit no-null add path received runtime nulls: slot={}, chunk={}",
            slot.getDescription(),
            getDescription());
      } else {
        BOLT_CHECK(
            input->scalar.nulls == nullptr,
            "HashAggrJit no-null add path received runtime nulls: slot={}, chunk={}",
            slot.getDescription(),
            getDescription());
      }
    }
  }

  bool allInputsUseIdentityMapping(char** inputRuntimes) const {
    for (auto i = 0; i < slots_.size(); ++i) {
      const auto& slot = slots_[i];
      if (slot.desc.isCountStar()) {
        continue;
      }
      auto* input =
          reinterpret_cast<const HashAggrJitInputRuntime*>(inputRuntimes[i]);
      if (slot.desc.inputShape() == HashAggrJitRuntimeShape::Row) {
        for (auto child = 0; child < input->row.numChildren; ++child) {
          if (!input->row.children[child]->isIdentityMapping) {
            return false;
          }
        }
      } else if (!input->scalar.isIdentityMapping) {
        return false;
      }
    }
    return true;
  }

  std::vector<HashAggrJitSlot> slots_;
  bool compileExtract_{true};
  std::string functionName_;
  CompiledModuleSP module_;
  HashAggrJitInitFunc init_{nullptr};
  HashAggrJitAddDenseFunc addDense_{nullptr};
  HashAggrJitAddDenseFunc addDenseNoNull_{nullptr};
  HashAggrJitAddDenseFunc addDenseNoNullIdentity_{nullptr};
  HashAggrJitExtractFunc extract_{nullptr};
  // Published last by codegen() (release) and read by isCodegenReady()
  // (acquire). Lets the query thread fall back to non-JIT while background
  // compilation is still in progress, then switch to JIT once ready.
  std::atomic<bool> ready_{false};
};

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
