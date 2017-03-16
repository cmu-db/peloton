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
#include "common/container_tuple.h"
#include "expression/constant_value_expression.h"
#include "planner/binding_context.h"

#include <vector>

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A wrapped class for output tuples
//===----------------------------------------------------------------------===//
class WrappedTuple
    : public expression::ContainerTuple<std::vector<type::Value>> {
 public:
  // Basic
  WrappedTuple(type::Value *vals, uint32_t num_vals)
      : ContainerTuple(&tuple_), tuple_(vals, vals + num_vals) {}

  // Copy
  WrappedTuple(const WrappedTuple &o)
      : ContainerTuple(&tuple_), tuple_(o.tuple_) {}
  WrappedTuple &operator=(const WrappedTuple &o) {
    expression::ContainerTuple<std::vector<type::Value>>::operator=(o);
    tuple_ = o.tuple_;
    return *this;
  }

  // The tuple
  std::vector<type::Value> tuple_;
 private:
};

//===----------------------------------------------------------------------===//
// A query consumer that buffers tuples into a local memory location
//===----------------------------------------------------------------------===//
class CodegenConsumer : public codegen::QueryResultConsumer {
 public:
  struct BufferingState {
    std::vector<WrappedTuple> *output;
  };

  // Constructor
  CodegenConsumer(std::vector<oid_t> cols, planner::BindingContext &context) {
    for (oid_t col_id : cols) {
      ais_.push_back(context.Find(col_id));
    }
    state.output = &tuples_;
  }

  void Prepare(codegen::CompilationContext &compilation_context) override;
  void InitializeState(codegen::CompilationContext &) override {}
  void TearDownState(codegen::CompilationContext &) override {}
  void ConsumeResult(codegen::ConsumerContext &ctx,
                     codegen::RowBatch::Row &row) const override;

  llvm::Value *GetStateValue(codegen::ConsumerContext &ctx,
                             const codegen::RuntimeState::StateID &id) const {
    auto &runtime_state = ctx.GetRuntimeState();
    return runtime_state.GetStateValue(ctx.GetCodeGen(), id);
  }
  
  struct _BufferTupleProxy {
    static llvm::Function *GetFunction(codegen::CodeGen &codegen);
  };

  // Called from query plan to buffer the tuple
  static void BufferTuple(char *state, type::Value *vals, uint32_t num_vals);

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
  // The ID of our output tuple buffer state
  codegen::RuntimeState::StateID tuple_output_state_id_;
};

}  // namespace test
}  // namespace peloton
