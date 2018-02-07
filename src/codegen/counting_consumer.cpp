//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// counting_consumer.cpp
//
// Identification: src/codegen/counting_consumer.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/counting_consumer.h"
#include "codegen/compilation_context.h"

namespace peloton {
namespace codegen {

void CountingConsumer::Prepare(codegen::CompilationContext &ctx) {
  // Be sure to call our parent
  ExecutionConsumer::Prepare(ctx);

  auto &codegen = ctx.GetCodeGen();
  auto &runtime_state = ctx.GetRuntimeState();
  counter_state_id_ = runtime_state.RegisterState(
      "consumerState", codegen.Int64Type()->getPointerTo());
}

void CountingConsumer::InitializeState(codegen::CompilationContext &context) {
  auto &codegen = context.GetCodeGen();
  auto *state_ptr = GetCounterState(codegen, context.GetRuntimeState());
  codegen->CreateStore(codegen.Const64(0), state_ptr);
}

// Increment the counter
void CountingConsumer::ConsumeResult(codegen::ConsumerContext &context,
                                     codegen::RowBatch::Row &) const {
  auto &codegen = context.GetCodeGen();

  auto *counter_ptr = GetCounterState(codegen, context.GetRuntimeState());
  auto *new_count =
      codegen->CreateAdd(codegen->CreateLoad(counter_ptr), codegen.Const64(1));
  codegen->CreateStore(new_count, counter_ptr);
}

llvm::Value *CountingConsumer::GetCounterState(
    codegen::CodeGen &codegen, codegen::RuntimeState &runtime_state) const {
  return runtime_state.LoadStateValue(codegen, counter_state_id_);
}

}  // namespace codegen
}  // namespace peloton