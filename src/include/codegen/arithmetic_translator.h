//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// arithmetic_translator.h
//
// Identification: src/include/codegen/arithmetic_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/expression_translator.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A translator of arithmetic expressions.
//===----------------------------------------------------------------------===//
class ArithmeticTranslator : public ExpressionTranslator {
 public:
  // Constructor
  ArithmeticTranslator(const expression::OperatorExpression &arithmetic,
                       CompilationContext &context)
      : ExpressionTranslator(arithmetic, context) {
    PL_ASSERT(arithmetic.GetChildrenSize() == 2);
  }

  // Produce the value that is the result of codegening the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override {
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
      default: {
        throw Exception(
            "Arithmetic expression has invalid type for translation: " +
            ExpressionTypeToString(arithmetic.GetExpressionType()));
      }
    }
  }
};

}  // namespace codegen
}  // namespace peloton