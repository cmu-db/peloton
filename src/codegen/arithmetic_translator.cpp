//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// arithmetic_translator.cpp
//
// Identification: src/codegen/arithmetic_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/arithmetic_translator.h"

#include "expression/operator_expression.h"

namespace peloton {
namespace codegen {

// Constructor
ArithmeticTranslator::ArithmeticTranslator(
    const expression::OperatorExpression &arithmetic,
    CompilationContext &context)
    : ExpressionTranslator(arithmetic, context) {
  PL_ASSERT(arithmetic.GetChildrenSize() == 2);
}

// Produce the value that is the result of codegening the expression
codegen::Value ArithmeticTranslator::DeriveValue(CodeGen &codegen,
                                                 RowBatch::Row &row) const {
  const auto &arithmetic = GetExpressionAs<expression::OperatorExpression>();
  codegen::Value left = row.DeriveValue(codegen, *arithmetic.GetChild(0));
  codegen::Value right = row.DeriveValue(codegen, *arithmetic.GetChild(1));

  switch (arithmetic.GetExpressionType()) {
    case ExpressionType::OPERATOR_PLUS:
      return left.Add(codegen, right);
    case ExpressionType::OPERATOR_MINUS:
      return left.Sub(codegen, right);
    case ExpressionType::OPERATOR_MULTIPLY:
      return left.Mul(codegen, right);
    case ExpressionType::OPERATOR_DIVIDE:
      return left.Div(codegen, right);
    case ExpressionType::OPERATOR_MOD:
      return left.Mod(codegen, right);
    case ExpressionType::OPERATOR_IS_NULL:
      if (arithmetic.get_nulltesttype() == NullTestType::IS_NULL) {
        LOG_INFO("In is null branch!");
        return left.IsNull(codegen, 0);
      } else {
        return left.IsNull(codegen, 1);
      }
    default: {
      throw Exception(
          "Arithmetic expression has invalid type for translation: " +
          ExpressionTypeToString(arithmetic.GetExpressionType()));
    }
  }
}

}  // namespace codegen
}  // namespace peloton