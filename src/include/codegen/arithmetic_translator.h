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
#include "codegen/consumer_context.h"
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
      : ExpressionTranslator(context), arithmetic_(arithmetic) {
    PL_ASSERT(arithmetic_.GetChildrenSize() == 2);

    // Prepare translators for the left and right expressions
    context.Prepare(*arithmetic_.GetChild(0));
    context.Prepare(*arithmetic_.GetChild(1));
  }

  // Produce the value that is the result of codegening the expression
  codegen::Value DeriveValue(ConsumerContext &context,
                             RowBatch::Row &row) const override {
    auto &codegen = GetCodeGen();
    codegen::Value left = context.DeriveValue(*arithmetic_.GetChild(0), row);
    codegen::Value right = context.DeriveValue(*arithmetic_.GetChild(1), row);

    switch (arithmetic_.GetExpressionType()) {
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
            "Invalid expression type for translation " +
            ExpressionTypeToString(arithmetic_.GetExpressionType()));
      }
    }
  }

 private:
  // The expression
  const expression::OperatorExpression &arithmetic_;
};

}  // namespace codegen
}  // namespace peloton