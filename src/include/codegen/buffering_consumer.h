//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffering_consumer.h
//
// Identification: src/include/codegen/buffering_consumer.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/query_result_consumer.h"
#include "codegen/value.h"
#include "common/container_tuple.h"

#include <vector>

namespace peloton {

namespace planner {
class BindingContext;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// A wrapped class for output tuples
//===----------------------------------------------------------------------===//
class WrappedTuple
    : public expression::ContainerTuple<std::vector<type::Value>> {
 public:
  // Basic Constructor
  WrappedTuple(type::Value *vals, uint32_t num_vals);

  // Copy Constructor
  WrappedTuple(const WrappedTuple &o);

  // Assignment
  WrappedTuple &operator=(const WrappedTuple &o);

  // The tuple
  std::vector<type::Value> tuple_;
};

//===----------------------------------------------------------------------===//
// A query consumer that buffers tuples into a local memory location
//===----------------------------------------------------------------------===//
class BufferingConsumer : public QueryResultConsumer {
 public:
  struct BufferingState {
    std::vector<WrappedTuple> *output;
  };

  // Constructor
  BufferingConsumer(const std::vector<oid_t> &cols,
                    planner::BindingContext &context);

  void Prepare(CompilationContext &compilation_context) override;
  void InitializeState(CompilationContext &) override {}
  void TearDownState(CompilationContext &) override {}
  void ConsumeResult(ConsumerContext &ctx, RowBatch::Row &row) const override;

  llvm::Value *GetStateValue(ConsumerContext &ctx,
                             const RuntimeState::StateID &id) const {
    auto &runtime_state = ctx.GetRuntimeState();
    return runtime_state.GetStateValue(ctx.GetCodeGen(), id);
  }

  // A proxy to BufferTuple(...)
  struct _BufferTupleProxy {
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // Called from compiled query code to buffer the tuple
  static void BufferTuple(char *state, type::Value *vals, uint32_t num_vals);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  BufferingState *GetState() { return &state; }

  const std::vector<WrappedTuple> &GetOutputTuples() const { return tuples_; }

 private:
  // The attributes we want to output
  std::vector<const planner::AttributeInfo *> output_ais_;

  // Buffered output tuples
  std::vector<WrappedTuple> tuples_;

  // Running buffering state
  BufferingState state;

  // The slot in the runtime state to find our state context
  RuntimeState::StateID consumer_state_id_;

  // The ID of our output tuple buffer state
  RuntimeState::StateID tuple_output_state_id_;
};

}  // namespace codegen
}  // namespace peloton