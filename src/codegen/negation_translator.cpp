//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// negation_translator.cpp
//
// Identification: src/codegen/negation_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/negation_translator.h"

#include "codegen/type/type_system.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace codegen {

// Constructor
NegationTranslator::NegationTranslator(
    const expression::OperatorUnaryMinusExpression &unary_minus_expression,
    CompilationContext &ctx)
    : ExpressionTranslator(unary_minus_expression, ctx) {
  PL_ASSERT(unary_minus_expression.GetChildrenSize() == 1);
}

Value NegationTranslator::DeriveValue(CodeGen &codegen,
                                      RowBatch::Row &row) const {
  const auto &negation_expr =
      GetExpressionAs<expression::OperatorUnaryMinusExpression>();
  auto child_value = row.DeriveValue(codegen, *negation_expr.GetChild(0));
  //  return child_value.CallUnary(codegen,
  //  type::TypeSystem::OperatorId::Negation);
  const auto &type_system = child_value.GetTypeSystem();
  const auto *negation_op = type_system.GetUnaryOperator(
      type::TypeSystem::OperatorId::Negation, child_value.GetType());
  return negation_op->DoWork(codegen, child_value);
}

}  // namespace codegen
}  // namespace peloton