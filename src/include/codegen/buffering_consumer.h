//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffering_consumer.h
//
// Identification: src/include/codegen/buffering_consumer.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <mutex>

#include "codegen/compilation_context.h"
#include "codegen/execution_consumer.h"
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

  std::string ToCSV() const;

  // The tuple
  std::vector<peloton::type::Value> tuple_;
};

//===----------------------------------------------------------------------===//
// A query consumer that buffers tuples into a local memory location
//===----------------------------------------------------------------------===//
class BufferingConsumer : public ExecutionConsumer {
 public:
  /// Constructor
  BufferingConsumer(const std::vector<oid_t> &cols,
                    const planner::BindingContext &context);

  void Prepare(CompilationContext &compilation_ctx) override;

  // Query state
  void InitializeQueryState(CompilationContext &) override {}
  void TearDownQueryState(CompilationContext &) override {}

  bool SupportsParallelExec() const override { return true; }

  void ConsumeResult(ConsumerContext &ctx, RowBatch::Row &row) const override;

  // Called from compiled query code to buffer the tuple
  static void BufferTuple(char *buffer, char *tuple, uint32_t num_cols);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  char *GetConsumerState() override;

  const std::vector<WrappedTuple> &GetOutputTuples() const;

 private:
  // The attributes we want to output
  std::vector<const planner::AttributeInfo *> output_ais_;

  // The thread-safe buffer of output tuples
  struct Buffer {
    std::mutex mutex;
    std::vector<WrappedTuple> output;
  };
  Buffer buffer_;

  // The slot in the runtime state to find our state context
  QueryState::Id consumer_state_id_;
};

}  // namespace codegen
}  // namespace peloton
