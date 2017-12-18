//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_translator.cpp
//
// Identification: src/codegen/operator/operator_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/operator_translator.h"

#include "codegen/compilation_context.h"

namespace peloton {
namespace codegen {

OperatorTranslator::OperatorTranslator(CompilationContext &context,
                                       Pipeline &pipeline)
    : context_(context), pipeline_(pipeline) {
  pipeline.Add(this);
}

CodeGen &OperatorTranslator::GetCodeGen() const {
  return context_.GetCodeGen();
}

llvm::Value *OperatorTranslator::GetStorageManagerPtr() const {
  return context_.GetStorageManagerPtr();
}

llvm::Value *OperatorTranslator::LoadStatePtr(
    const RuntimeState::StateID &state_id) const {
  auto &runtime_state = context_.GetRuntimeState();
  return runtime_state.LoadStatePtr(GetCodeGen(), state_id);
}

llvm::Value *OperatorTranslator::LoadStateValue(
    const RuntimeState::StateID &state_id) const {
  auto &runtime_state = context_.GetRuntimeState();
  return runtime_state.LoadStateValue(GetCodeGen(), state_id);
}

void OperatorTranslator::Consume(ConsumerContext &context,
                                 RowBatch &batch) const {
  batch.Iterate(GetCodeGen(), [this, &context](RowBatch::Row &row) {
    Consume(context, row);
  });
}

}  // namespace codegen
}  // namespace peloton
