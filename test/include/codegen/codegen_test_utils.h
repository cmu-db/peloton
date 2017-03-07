//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen_test_utils.h
//
// Identification: test/include/codegen/codegen_test_utils.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/query_result_consumer.h"
#include "codegen/value.h"
#include "expression/constant_value_expression.h"
#include "expression/container_tuple.h"
#include "planner/binding_context.h"

#include <vector>

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// Common utilities
//===----------------------------------------------------------------------===//
class CodegenTestUtils {
 public:
  static expression::ConstantValueExpression *CreateConstantIntExpression(
      int64_t val);
};

//===----------------------------------------------------------------------===//
// A query consumer that prints the tuples to standard out
//===----------------------------------------------------------------------===//
class Printer : public codegen::QueryResultConsumer {
 public:
  Printer(std::vector<oid_t> cols, planner::BindingContext &context) {
    for (oid_t col_id : cols) {
      ais_.push_back(context.Find(col_id));
    }
  }

  // None of these are used, except ConsumeResult()
  void Prepare(codegen::CompilationContext &) override {}
  void PrepareResult(codegen::CompilationContext &) override {}
  void InitializeState(codegen::CompilationContext &) override {}
  void TearDownState(codegen::CompilationContext &) override {}
  // Use
  void ConsumeResult(codegen::ConsumerContext &ctx,
                     codegen::RowBatch::Row &) const override;

 private:
  std::vector<const planner::AttributeInfo *> ais_;
};

//===----------------------------------------------------------------------===//
// A query consumer that buffers tuples into a local buffer
//===----------------------------------------------------------------------===//
/*
class WrappedTuple : AbstractTuple {
 public:
  WrappedTuple(Value* vals, uint32_t num_vals) : vals_(vals, vals + num_vals) {}
  WrappedTuple(std::vector<Value> vals) : vals_(vals) {}

  Value GetValue(oid_t col_id) const override { return vals_[col_id]; }

  char* GetData() const { return (char*)vals_.data(); }

 private:
  std::vector<Value> vals_;
};
 */

class WrappedTuple : public expression::ContainerTuple<std::vector<Value>> {
 public:
  // Basic
  WrappedTuple(Value *vals, uint32_t num_vals)
      : ContainerTuple(&vals_), vals_(vals, vals + num_vals) {}

  // Copy
  WrappedTuple(const WrappedTuple &o)
      : ContainerTuple(&vals_), vals_(o.vals_) {}
  WrappedTuple &operator=(const WrappedTuple &o) {
    expression::ContainerTuple<std::vector<Value>>::operator=(o);
    vals_ = o.vals_;
    return *this;
  }

 private:
  std::vector<Value> vals_;
};

//===----------------------------------------------------------------------===//
// A query consumer that buffers tuples into a local buffer
//===----------------------------------------------------------------------===//
class BufferingConsumer : public codegen::QueryResultConsumer {
 public:
  struct BufferingState {
    std::vector<WrappedTuple> *output;
  };

  // Constructor
  BufferingConsumer(std::vector<oid_t> cols, planner::BindingContext &context) {
    for (oid_t col_id : cols) {
      ais_.push_back(context.Find(col_id));
    }
    state.output = &tuples_;
  }

  void Prepare(codegen::CompilationContext &compilation_context) override;
  void PrepareResult(codegen::CompilationContext &ctx) override;
  void InitializeState(codegen::CompilationContext &) override {}
  void TearDownState(codegen::CompilationContext &) override {}
  void ConsumeResult(codegen::ConsumerContext &ctx,
                     codegen::RowBatch::Row &row) const override;

  llvm::Value *GetConsumerState(codegen::ConsumerContext &ctx) const {
    auto &runtime_state = ctx.GetRuntimeState();
    return runtime_state.GetStateValue(ctx.GetCodeGen(), consumer_state_id_);
  }

  struct _BufferTupleProxy {
    static llvm::Function *GetFunction(codegen::CodeGen &codegen);
  };

  // Called from query plan to buffer the tuple
  static void BufferTuple(char *state, Value *vals, uint32_t num_vals);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  BufferingState *GetState() { return &state; }

  const std::vector<WrappedTuple> &GetOutputTuples() const { return tuples_; }

 private:
  //
  std::vector<const planner::AttributeInfo *> ais_;
  // Buffered output tuples
  std::vector<WrappedTuple> tuples_;
  // Tuple space
  llvm::Value *tuple_buffer_;
  // Running buffering state
  BufferingState state;
  // The slot in the runtime state to find our state context
  codegen::RuntimeState::StateID consumer_state_id_;
};

//===----------------------------------------------------------------------===//
// A consumer that just counts the number of results
//===----------------------------------------------------------------------===//
class CountingConsumer : public codegen::QueryResultConsumer {
 public:
  void Prepare(codegen::CompilationContext &compilation_context) override;
  void PrepareResult(codegen::CompilationContext &) override {}
  void InitializeState(codegen::CompilationContext &context) override;
  void ConsumeResult(codegen::ConsumerContext &context,
                     codegen::RowBatch::Row &row) const override;
  void TearDownState(codegen::CompilationContext &) override {}

  uint64_t GetCount() const { return counter_; }

 private:
  llvm::Value *GetCounter(codegen::CodeGen &codegen,
                          codegen::RuntimeState &runtime_state) const {
    return runtime_state.GetStateValue(codegen, counter_state_id_);
  }

  llvm::Value *GetCounter(codegen::ConsumerContext &ctx) const {
    return GetCounter(ctx.GetCodeGen(), ctx.GetRuntimeState());
  }

 private:
  uint64_t counter_;
  // The slot in the runtime state to find our state context
  codegen::RuntimeState::StateID counter_state_id_;
};

}  // namespace test
}  // namespace peloton