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
      : ExpressionTranslator(comparison, context) {
    PL_ASSERT(comparison.GetChildrenSize() == 2);
  }

  // Produce the result of performing the comparison of left and right values
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override {
    const auto &comparison =
        GetExpressionAs<expression::ComparisonExpression>();

    codegen::Value left = row.DeriveValue(codegen, *comparison.GetChild(0));
    codegen::Value right = row.DeriveValue(codegen, *comparison.GetChild(1));

    switch (comparison.GetExpressionType()) {
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
            ExpressionTypeToString(comparison.GetExpressionType())};
      }
    }
  }
};

}  // namespace codegen
}  // namespace peloton