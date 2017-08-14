#include "codegen/expression/function_translator.h"

#include "expression/function_expression.h"

namespace peloton {
namespace codegen {

// Constructor
FunctionTranslator::FunctionTranslator(
    const expression::FunctionExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {}

codegen::Value FunctionTranslator::DeriveValue(
    CodeGen &codegen, RowBatch::Row &row) const {
  const auto &func_expr = GetExpressionAs<expression::FunctionExpression>();
  func_expr->
}

}  // namespace codegen
}  // namespace peloton
