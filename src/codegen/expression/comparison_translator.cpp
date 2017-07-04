//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// comparison_translator.cpp
//
// Identification: src/codegen/expression/comparison_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/comparison_translator.h"

#include "expression/comparison_expression.h"

namespace peloton {
namespace codegen {

// Constructor
ComparisonTranslator::ComparisonTranslator(
    const expression::ComparisonExpression &comparison,
    CompilationContext &context)
    : ExpressionTranslator(comparison, context) {
  PL_ASSERT(comparison.GetChildrenSize() == 2);
}

// Produce the result of performing the comparison of left and right values
codegen::Value ComparisonTranslator::DeriveValue(CodeGen &codegen,
                                                 RowBatch::Row &row) const {
  const auto &comparison = GetExpressionAs<expression::ComparisonExpression>();

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
      throw Exception{"Invalid expression type for translation " +
                      ExpressionTypeToString(comparison.GetExpressionType())};
    }
  }
}

}  // namespace codegen
}  // namespace peloton