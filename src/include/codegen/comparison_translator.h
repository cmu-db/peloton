//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// comparison_translator.h
//
// Identification: src/include/codegen/comparison_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/expression_translator.h"
#include "expression/comparison_expression.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A translator of comparison expressions.
//===----------------------------------------------------------------------===//
class ComparisonTranslator : public ExpressionTranslator {
 public:
  // Constructor
  ComparisonTranslator(const expression::ComparisonExpression &comparison,
                       CompilationContext &context)
      : ExpressionTranslator(context), comparison_(comparison) {
    PL_ASSERT(comparison_.GetChildrenSize() == 2);

    // Prepare translators for the left and right expressions
    context.Prepare(*comparison_.GetChild(0));
    context.Prepare(*comparison_.GetChild(1));
  }

  // Produce the result of performing the comparison of left and right values
  codegen::Value DeriveValue(ConsumerContext &context,
                             RowBatch::Row &row) const override {
    auto &codegen = GetCodeGen();
    codegen::Value left = context.DeriveValue(*comparison_.GetChild(0), row);
    codegen::Value right = context.DeriveValue(*comparison_.GetChild(1), row);

    switch (comparison_.GetExpressionType()) {
      case ExpressionType::COMPARE_EQUAL:
        return left.CompareEq(codegen, right);
      case ExpressionType::COMPARE_NOTEQUAL:
        return left.CompareNe(codegen, right);
      case ExpressionType::COMPARE_LESSTHAN:
        return left.CompareLt(codegen, right);
      case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        return left.CompareLte(codegen, right);
      case ExpressionType::COMPARE_GREATERTHAN:
        return left.CompareGt(codegen, right);
      case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        return left.CompareGte(codegen, right);
      default: {
        throw Exception{
            "Invalid expression type for translation " +
            ExpressionTypeToString(comparison_.GetExpressionType())};
      }
    }
  }

 private:
  // The expression
  const expression::ComparisonExpression &comparison_;
};

}  // namespace codegen
}  // namespace peloton