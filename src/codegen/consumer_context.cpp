//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// consumer_context.cpp
//
// Identification: src/codegen/consumer_context.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/consumer_context.h"

#include "codegen/compilation_context.h"

namespace peloton {
namespace codegen {

// Constructor
ConsumerContext::ConsumerContext(CompilationContext &compilation_context,
                                 llvm::Value *task_id,
                                 Pipeline &pipeline)
    : compilation_context_(compilation_context), pipeline_(pipeline),
      task_id_(task_id) {}

// Pass the row batch to the next operator in the pipeline
void ConsumerContext::Consume(RowBatch &batch) {
  auto *translator = pipeline_.NextStep();
  if (translator == nullptr) {
    // We're at the end of the query pipeline, we now send the output tuples
    // to the result consumer configured in the compilation context
    auto &consumer = compilation_context_.GetQueryResultConsumer();
    consumer.ConsumeResult(*this, task_id_, batch);
  } else {
    // We're not at the end of the pipeline, push the batch through the stages
    do {
      translator->Consume(*this, batch);
      // When the call returns here, the pipeline position has been shifted to
      // the start of a new stage.
    } while ((translator = pipeline_.NextStep()) != nullptr);
  }
}

// Pass this row to the next operator in the pipeline
void ConsumerContext::Consume(RowBatch::Row &row) {
  // If we're at a stage boundary in the pipeline, it means the next operator
  // in the pipeline wants to operate on a batch of rows. To facilitate this,
  // we mark the given row as valid in this batch and return immediately.
  if (pipeline_.AtStageBoundary()) {
    auto &codegen = GetCodeGen();
    row.SetValidity(codegen, codegen.ConstBool(true));
    return;
  }

  // Otherwise, we move along to the next operator in the pipeline and deliver
  // the row there.
  auto *translator = pipeline_.NextStep();
  if (translator != nullptr) {
    translator->Consume(*this, row);
    return;
  }

  // We're at the end of the query pipeline, we now send the output tuples
  // to the result consumer configured in the compilation context
  auto &consumer = compilation_context_.GetQueryResultConsumer();
  consumer.ConsumeResult(*this, task_id_, row);
}

void ConsumerContext::NotifyNumTasks(llvm::Value *ntasks) {
  if (pipeline_.AtStageBoundary()) {
    PL_ASSERT(false && "Not Implemented");
    return;
  }
  if (pipeline_.PeekNextStep() != nullptr) {
    PL_ASSERT(false && "Not Implemented");
  }

  auto &consumer = compilation_context_.GetQueryResultConsumer();
  consumer.CodeGenNotifyNumTasks(compilation_context_, ntasks);
}

CodeGen &ConsumerContext::GetCodeGen() const {
  return compilation_context_.GetCodeGen();
}

RuntimeState &ConsumerContext::GetRuntimeState() const {
  return compilation_context_.GetRuntimeState();
}

}  // namespace codegen
}  // namespace peloton