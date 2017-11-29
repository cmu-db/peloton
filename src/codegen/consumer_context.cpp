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
#include "codegen/function_builder.h"
#include "codegen/proxy/task_info_proxy.h"

namespace peloton {
namespace codegen {

// Constructor
ConsumerContext::ConsumerContext(CompilationContext &compilation_context,
                                 Pipeline &pipeline)
    : compilation_context_(compilation_context), pipeline_(pipeline) {}

// Pass the row batch to the next operator in the pipeline
void ConsumerContext::Consume(RowBatch &batch) {
  auto *translator = pipeline_.NextStep();
  if (translator == nullptr) {
    // We're at the end of the query pipeline, we now send the output tuples
    // to the result consumer configured in the compilation context
    auto &consumer = compilation_context_.GetQueryResultConsumer();
    consumer.ConsumeResult(*this, batch);
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
  consumer.ConsumeResult(*this, row);
}

CodeGen &ConsumerContext::GetCodeGen() const {
  return compilation_context_.GetCodeGen();
}

RuntimeState &ConsumerContext::GetRuntimeState() const {
  return compilation_context_.GetRuntimeState();
}

llvm::Value *ConsumerContext::GetTaskId() const {
  auto &codegen = GetCodeGen();

  if (!compilation_context_.MultithreadOn()) {
    return codegen.Const32(0);
  } else {
    auto task_info_ptr =
        codegen.GetCodeContext().GetCurrentFunction()->GetArgumentByPosition(1);
    return codegen.Call(TaskInfoProxy::GetTaskId, {task_info_ptr});
  }
}

}  // namespace codegen
}  // namespace peloton