//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// execution_consumer.h
//
// Identification: src/include/codegen/execution_consumer.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/consumer_context.h"
#include "codegen/parameter_cache.h"
#include "codegen/query_parameters_map.h"

namespace peloton {
namespace codegen {

/**
 * This is the main abstract class for consumers of query executions.
 */
class ExecutionConsumer {
 public:
  /// Default constructor
  ExecutionConsumer();

  /// Default virtual destructor
  virtual ~ExecutionConsumer() = default;

  /// Does this consumer support parallel execution?
  virtual bool SupportsParallelExec() const = 0;

  /// Invoked before code-generation begins to allow the consumer to prepare
  /// itself in the provided context
  virtual void Prepare(CompilationContext &compilation_ctx);

  /// Called to generate any initialization code the consumer needs
  virtual void InitializeQueryState(CompilationContext &compilation_ctx) = 0;

  /// Called to generate any code to tear down the state of the consumer
  virtual void TearDownQueryState(CompilationContext &compilation_ctx) = 0;

  /// Pipeline-related operations. These functions are invoked exactly once for
  /// each pipeline the operator is part of.
  virtual void RegisterPipelineState(PipelineContext &) {}
  virtual void InitializePipelineState(PipelineContext &pipeline_ctx);
  virtual void FinishPipeline(PipelineContext &) {}
  virtual void TearDownPipelineState(PipelineContext &) {}

  ///
  virtual char *GetConsumerState() = 0;

  /// Consume either a single output (i.e., result) row, or a batch of output
  /// rows. A default implementation is provided by batch-consumption, but one
  /// must be provided for single-row consumption.
  virtual void ConsumeResult(ConsumerContext &context, RowBatch &batch) const;
  virtual void ConsumeResult(ConsumerContext &context,
                             RowBatch::Row &row) const = 0;

  /// Load the ExecutorContext, Transaction, or StorageManager objects
  llvm::Value *GetExecutorContextPtr(CompilationContext &compilation_ctx);
  llvm::Value *GetTransactionPtr(CompilationContext &compilation_ctx);
  llvm::Value *GetStorageManagerPtr(CompilationContext &compilation_ctx);
  llvm::Value *GetQueryParametersPtr(CompilationContext &compilation_ctx);
  llvm::Value *GetThreadStatesPtr(CompilationContext &compilation_ctx);

 private:
  llvm::Type *executor_ctx_type_;
  QueryState::Id executor_ctx_id_;
};

}  // namespace codegen
}  // namespace peloton
