//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// case_translator.cpp
//
// Identification: src/codegen/case_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/case_translator.h"
#include "codegen/compilation_context.h"
#include "codegen/if.h"

namespace peloton {
namespace codegen {

/*
CaseTranslator::CaseTranslator(
    const expression::CaseExpression &case_expression,
    CompilationContext &context)
    : ExpressionTranslator(context), case_expression_(case_expression) {
  // We need to prepare each component of the case
  for (const auto &clause : case_expression_.GetClauses()) {
    context.Prepare(*clause.first);
    context.Prepare(*clause.second);
  }
  if (case_expression_.GetDefault() != nullptr) {
    context.Prepare(*case_expression_.GetDefault());
  }
}

codegen::Value CaseTranslator::DeriveValue(ConsumerContext &context,
                                           RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();

  llvm::BasicBlock *merge_bb =
      llvm::BasicBlock::Create(codegen.GetContext(), "caseMerge");

  std::vector<std::pair<codegen::Value, llvm::BasicBlock *>> branch_vals;

  const auto &clauses = case_expression_.GetClauses();
  for (uint32_t i = 0; i < clauses.size(); i++) {
    codegen::Value cond = context.DeriveValue(*clauses[i].first, row);
    If when{codegen, cond.GetValue(), "case" + std::to_string(i)};
    {
      codegen::Value ret = context.DeriveValue(*clauses[i].second, row);
      branch_vals.emplace_back(ret, codegen->GetInsertBlock());
    }
    when.EndIf(merge_bb);
  }

  // If there's a default clause, compute it
  if (case_expression_.GetDefault() != nullptr) {
    codegen::Value
        default_ret = context.DeriveValue(*case_expression_.GetDefault(), row);
    branch_vals.emplace_back(default_ret, codegen->GetInsertBlock());
  }

  // Push the merging block to the end. Let's continue from there.
  auto *func = codegen->GetInsertBlock()->getParent();
  func->getBasicBlockList().push_back(merge_bb);
  codegen->SetInsertPoint(merge_bb);

  // Return the single node combining all possible branch values
  return codegen::Value::BuildPHI(GetCodeGen(), branch_vals);
}
*/

}  // namespace codegen
}  // namespace peloton