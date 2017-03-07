//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_translator.cpp
//
// Identification: src/codegen/operator_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator_translator.h"

#include "codegen/compilation_context.h"

namespace peloton {
namespace codegen {

OperatorTranslator::OperatorTranslator(CompilationContext &context,
                                       Pipeline &pipeline)
    : context_(context), pipeline_(pipeline) {
  pipeline.AddStep(this);
}

CodeGen &OperatorTranslator::GetCodeGen() const {
  return context_.GetCodeGen();
}

llvm::Value *OperatorTranslator::GetManagerPtr() const {
  return context_.GetManagerPtr();
}

llvm::Value *OperatorTranslator::GetStatePtr(
    const RuntimeState::StateID &state_id) const {
  auto &runtime_state = context_.GetRuntimeState();
  return runtime_state.GetStatePtr(GetCodeGen(), state_id);
}

llvm::Value *OperatorTranslator::GetStateValue(
    const RuntimeState::StateID &state_id) const {
  auto &runtime_state = context_.GetRuntimeState();
  return runtime_state.GetStateValue(GetCodeGen(), state_id);
}

void OperatorTranslator::Consume(ConsumerContext &context,
                                 RowBatch &batch) const {
  batch.Iterate(context.GetCodeGen(), [this, &context](RowBatch::Row &row) {
    Consume(context, row);
  });
}

}  // namespace codegen
}  // namespace peloton