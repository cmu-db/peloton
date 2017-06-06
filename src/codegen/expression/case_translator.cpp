//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// case_translator.cpp
//
// Identification: src/codegen/expression/case_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/case_translator.h"

#include "codegen/compilation_context.h"
#include "include/codegen/utils/if.h"
#include "codegen/type.h"

namespace peloton {
namespace codegen {

CaseTranslator::CaseTranslator(
    const expression::CaseExpression &expression, CompilationContext &context)
    : ExpressionTranslator(expression, context) {

  // We need to prepare each component of the case
  for (const auto &clause : expression.GetWhenClauses()) {
    context.Prepare(*clause.first);
    context.Prepare(*clause.second);
  }
  if (expression.GetDefault() != nullptr) {
    context.Prepare(*expression.GetDefault());
  }
}

codegen::Value CaseTranslator::DeriveValue(CodeGen &codegen,
                                           RowBatch::Row &row) const {

  // Might not appear in the output IR when all If's are optimized to Switch's
  // Potential future enhancement: Consider using Switch instead of If
  //                               It may provide more code readability
  llvm::BasicBlock *merge_bb =
      llvm::BasicBlock::Create(codegen.GetContext(), "caseMerge");

  std::vector<std::pair<codegen::Value, llvm::BasicBlock *>> branch_vals;

  const auto &expr = GetExpressionAs<expression::CaseExpression>();

  // Handle all the When Cluases
  codegen::Value ret;
  for (uint32_t i = 0; i < expr.GetWhenClauseSize(); i++) {
    codegen::Value cond = row.DeriveValue(codegen, *expr.GetWhenClauseCond(i));
    If when{codegen, cond.GetValue(), "case" + std::to_string(i)};
    {
      ret = row.DeriveValue(codegen, *expr.GetWhenClauseResult(i));
      branch_vals.emplace_back(ret, codegen->GetInsertBlock());
    }
    when.EndIf(merge_bb);
  }
  // Jump to the merging block from the internal If merging block
  codegen->CreateBr(merge_bb);

  // Compute the default clause
  // default_ret will have the same type as one of the ret's from above
  codegen::Value default_ret = expr.GetDefault() != nullptr ?
      row.DeriveValue(codegen, *expr.GetDefault()) :
      Type::GetNullValue(codegen, ret.GetType());
  branch_vals.emplace_back(default_ret, codegen->GetInsertBlock());

  // Push the merging block to the end and build the PHI on this merging block
  auto *func = codegen->GetInsertBlock()->getParent();
  func->getBasicBlockList().push_back(merge_bb);
  codegen->SetInsertPoint(merge_bb);

  // Return the single node combining all possible branch values
  return codegen::Value::BuildPHI(codegen, branch_vals);
}

}  // namespace codegen
}  // namespace peloton
