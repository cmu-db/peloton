//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// conjunction_translator.h
//
// Identification: src/include/codegen/conjunction_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/expression_translator.h"
#include "expression/conjunction_expression.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for conjunction expressions
//===----------------------------------------------------------------------===//
class ConjunctionTranslator : public ExpressionTranslator {
 public:
  // Constructor
  ConjunctionTranslator(const expression::ConjunctionExpression &conjunction,
                        CompilationContext &context)
      : ExpressionTranslator(conjunction, context) {
    PL_ASSERT(conjunction.GetChildrenSize() == 2);
  }

  // Produce the value that is the result of codegening the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override {
    const auto &conjunction =
        GetExpressionAs<expression::ConjunctionExpression>();
    codegen::Value left = row.DeriveValue(codegen, *conjunction.GetChild(0));
    codegen::Value right = row.DeriveValue(codegen, *conjunction.GetChild(1));

    switch (conjunction.GetExpressionType()) {
      case ExpressionType::CONJUNCTION_AND:
        return left.LogicalAnd(codegen, right);
      case ExpressionType::CONJUNCTION_OR:
        return left.LogicalOr(codegen, right);
      default:
        throw Exception{
            "Received a non-conjunction expression type: " +
            ExpressionTypeToString(conjunction.GetExpressionType())};
    }
  }
};

}  // namespace codegen
}  // namespace peloton