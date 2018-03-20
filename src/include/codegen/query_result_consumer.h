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
  virtual void ConsumeResult(ConsumerContext &context,
                             RowBatch::Row &row) const = 0;

  virtual char *GetConsumerState() = 0;

  void ConsumeResult(ConsumerContext &context, RowBatch &batch) const {
    batch.Iterate(context.GetCodeGen(), [this, &context](RowBatch::Row &row) {
      ConsumeResult(context, row);
    });
  }

  // Called to generate any code to tear down the state of the consumer
  virtual void TearDownState(CompilationContext &compilation_context) = 0;
};

}  // namespace codegen
}  // namespace peloton