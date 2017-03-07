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
#include "codegen/consumer_context.h"
#include "codegen/expression_translator.h"
#include "expression/conjunction_expression.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
/// A translator for conjunction expressions
//===----------------------------------------------------------------------===//
class ConjunctionTranslator : public ExpressionTranslator {
 public:
  // Constructor
  ConjunctionTranslator(const expression::ConjunctionExpression &conjunction,
                        CompilationContext &context)
      : ExpressionTranslator(context), conjunction_(conjunction) {
    PL_ASSERT(conjunction_.GetChildrenSize() == 2);

    // Prepare translators for the left and right expressions
    context.Prepare(*conjunction_.GetChild(0));
    context.Prepare(*conjunction_.GetChild(1));
  }

  // Produce the value that is the result of codegening the expression
  codegen::Value DeriveValue(ConsumerContext &context,
                             RowBatch::Row &row) const override {
    auto &codegen = GetCodeGen();
    codegen::Value left = context.DeriveValue(*conjunction_.GetChild(0), row);
    codegen::Value right = context.DeriveValue(*conjunction_.GetChild(1), row);

    switch (conjunction_.GetExpressionType()) {
      case ExpressionType::CONJUNCTION_AND:
        return left.LogicalAnd(codegen, right);
      case ExpressionType::CONJUNCTION_OR:
        return left.LogicalOr(codegen, right);
      default:
        throw Exception{
            "Received a non-conjunction expression type: " +
            ExpressionTypeToString(conjunction_.GetExpressionType())};
    }
  }

 private:
  // The expression
  const expression::ConjunctionExpression &conjunction_;
};

}  // namespace codegen
}  // namespace peloton