//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_translator.cpp
//
// Identification: src/codegen/operator/operator_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/operator_translator.h"

#include "codegen/compilation_context.h"

namespace peloton {
namespace codegen {

OperatorTranslator::OperatorTranslator(const planner::AbstractPlan &plan,
                                       CompilationContext &context,
                                       Pipeline &pipeline)
    : plan_(plan), context_(context), pipeline_(pipeline) {
  // Add this operator to the provided pipeline
  pipeline.Add(this);
}

CodeGen &OperatorTranslator::GetCodeGen() const {
  return context_.GetCodeGen();
}

llvm::Value *OperatorTranslator::GetExecutorContextPtr() const {
  return context_.GetExecutionConsumer().GetExecutorContextPtr(context_);
}

llvm::Value *OperatorTranslator::GetTransactionPtr() const {
  return context_.GetExecutionConsumer().GetTransactionPtr(context_);
}

llvm::Value *OperatorTranslator::GetStorageManagerPtr() const {
  return context_.GetExecutionConsumer().GetStorageManagerPtr(context_);
}

llvm::Value *OperatorTranslator::LoadStatePtr(
    const QueryState::Id &state_id) const {
  QueryState &query_state = context_.GetQueryState();
  return query_state.LoadStatePtr(GetCodeGen(), state_id);
}

llvm::Value *OperatorTranslator::LoadStateValue(
    const QueryState::Id &state_id) const {
  QueryState &query_state = context_.GetQueryState();
  return query_state.LoadStateValue(GetCodeGen(), state_id);
}

void OperatorTranslator::Consume(ConsumerContext &context,
                                 RowBatch &batch) const {
  batch.Iterate(GetCodeGen(), [this, &context](RowBatch::Row &row) {
    Consume(context, row);
  });
}

}  // namespace codegen
}  // namespace peloton
