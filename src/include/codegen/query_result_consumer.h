//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_result_consumer.h
//
// Identification: src/include/codegen/query_result_consumer.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/consumer_context.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// The main interface for consumers of query results.
//===----------------------------------------------------------------------===//
class QueryResultConsumer {
 public:
  // Destructor
  virtual ~QueryResultConsumer() = default;

  // Let the consumer perform any initialization or state declarations here
  virtual void Prepare(CompilationContext &compilation_context) = 0;

  // Called to generate any initialization code the consumer needs
  virtual void InitializeState(CompilationContext &compilation_context) = 0;

  // Called during plan-generation to consume the results of the query
  virtual void ConsumeResult(ConsumerContext &context, llvm::Value *task_id,
                             RowBatch::Row &row) const = 0;

  // Get the state that's accessible at runtime
  virtual char *GetConsumerState() = 0;

  // Generate code that notifies the consumer about the number of tasks
  virtual void CodeGenNotifyNumTasks(CompilationContext &context,
                                     llvm::Value *ntasks) = 0;

  void ConsumeResult(ConsumerContext &context, llvm::Value *task_id,
                     RowBatch &batch) const {
    batch.Iterate(context.GetCodeGen(),
                  [this, &context, task_id](RowBatch::Row &row) {
                    ConsumeResult(context, task_id, row);
                  });
  }

  // Called to generate any code to tear down the state of the consumer
  virtual void TearDownState(CompilationContext &compilation_context) = 0;
};

}  // namespace codegen
}  // namespace peloton