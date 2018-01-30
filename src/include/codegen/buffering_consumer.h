//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffering_consumer.h
//
// Identification: src/include/codegen/buffering_consumer.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "codegen/compilation_context.h"
#include "codegen/query_result_consumer.h"
#include "codegen/value.h"
#include "common/container_tuple.h"

namespace peloton {

namespace planner {
class BindingContext;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// A wrapped class for output tuples
//===----------------------------------------------------------------------===//
class WrappedTuple : public ContainerTuple<std::vector<peloton::type::Value>> {
 public:
  // Basic Constructor
  WrappedTuple(peloton::type::Value *vals, uint32_t num_vals);

  // Copy Constructor
  WrappedTuple(const WrappedTuple &o);

  // Assignment
  WrappedTuple &operator=(const WrappedTuple &o);

  // The tuple
  std::vector<peloton::type::Value> tuple_;
};

//===----------------------------------------------------------------------===//
// A query consumer that buffers tuples into a local memory location
//===----------------------------------------------------------------------===//
class BufferingConsumer : public QueryResultConsumer {
 public:
  struct BufferingState {
    std::vector<std::vector<WrappedTuple>> *outputs;
  };

  // Constructor
  BufferingConsumer(const std::vector<oid_t> &cols,
                    const planner::BindingContext &context);

  void Prepare(CompilationContext &compilation_context) override;
  void InitializeState(CompilationContext &) override {}
  void TearDownState(CompilationContext &) override {}
  void ConsumeResult(ConsumerContext &ctx, llvm::Value *task_id,
                     RowBatch::Row &row) const override;

  llvm::Value *GetStateValue(ConsumerContext &ctx,
                             const RuntimeState::StateID &id) const {
    auto &runtime_state = ctx.GetRuntimeState();
    return runtime_state.LoadStateValue(ctx.GetCodeGen(), id);
  }

  // Called from compiled query code to buffer the tuple
  static void BufferTuple(char *state, char *tuple, uint32_t num_cols,
                          uint64_t task_id);

  // Called from compiled query code to prepare per-task buffer
  static void NotifyNumTasks(char *state, size_t ntasks);

  void CodeGenNotifyNumTasks(CompilationContext &context,
                             llvm::Value *ntasks) final;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  char *GetConsumerState() final { return reinterpret_cast<char *>(&state); }

  const std::vector<std::vector<WrappedTuple>> &GetOutputPartitions() const {
    return tuples_;
  }

  const std::vector<WrappedTuple> &GetOutputTuples() const {
    if (merged_outputs_.empty()) {
      for (auto &partition : GetOutputPartitions()) {
        for (auto &tuple : partition) {
          merged_outputs_.push_back(tuple);
        }
      }
    }
    return merged_outputs_;
  }

 private:
  // The attributes we want to output
  std::vector<const planner::AttributeInfo *> output_ais_;

  // Buffered output tuples
  std::vector<std::vector<WrappedTuple>> tuples_;

  mutable std::vector<WrappedTuple> merged_outputs_;

  // Running buffering state
  BufferingState state;

  // The slot in the runtime state to find our state context
  RuntimeState::StateID consumer_state_id_;
};

}  // namespace codegen
}  // namespace peloton
