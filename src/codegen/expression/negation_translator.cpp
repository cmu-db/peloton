//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// negation_translator.cpp
//
// Identification: src/codegen/expression/negation_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/negation_translator.h"

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

  codegen::Value child_value =
      row.DeriveValue(codegen, *negation_expr.GetChild(0));

  codegen::Value zero{type::Type::TypeId::INTEGER, codegen.Const32(0)};

  return zero.Sub(codegen, child_value);
}

}  // namespace codegen
}  // namespace peloton