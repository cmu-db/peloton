//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// consumer_context.h
//
// Identification: src/include/codegen/consumer_context.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/pipeline.h"
#include "codegen/query_state.h"
#include "codegen/row_batch.h"
#include "codegen/value.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}  // namespace expression

namespace codegen {

// Forward declare
class CompilationContext;
class PipelineContext;

//===----------------------------------------------------------------------===//
// This is just a glue class that manages the given pipeline and provides access
// to the compilation context. In the future, we need to modify this class to
// track parallel state since parallelism will be based on pipelines.
//===----------------------------------------------------------------------===//
class ConsumerContext {
 public:
  // Constructor
  ConsumerContext(CompilationContext &compilation_context, Pipeline &pipeline);
  ConsumerContext(CompilationContext &compilation_context, Pipeline &pipeline,
                  PipelineContext *pipeline_context);

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(ConsumerContext);

  // Pass this consumer context to the parent of the caller of consume()
  void Consume(RowBatch &batch);
  void Consume(RowBatch::Row &row);

  CompilationContext &GetCompilationContext() { return compilation_context_; }

  // Get the code generator instance
  CodeGen &GetCodeGen() const;

  // Get the query state
  QueryState &GetQueryState() const;

  // Get the pipeline
  const Pipeline &GetPipeline() const { return pipeline_; }

 private:
  // The compilation context
  CompilationContext &compilation_context_;

  // The pipeline of operators that this context passes through
  Pipeline &pipeline_;
  PipelineContext *pipeline_context_;
};

}  // namespace codegen
}  // namespace peloton