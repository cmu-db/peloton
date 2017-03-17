//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_value_translator.h
//
// Identification: src/include/codegen/tuple_value_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression_translator.h"
#include "expression/tuple_value_expression.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for expressions that access a specific attribute in a tuple
//===----------------------------------------------------------------------===//
class TupleValueTranslator : public ExpressionTranslator {
 public:
  // Constructor
  TupleValueTranslator(const expression::TupleValueExpression &tve_expr,
                       CompilationContext &context)
      : ExpressionTranslator(tve_expr, context) {
    PL_ASSERT(tve_expr.GetAttributeRef() != nullptr);
  }

  // Produce the value that is the result of codegening the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override {
    const auto &tve_expr = GetExpressionAs<expression::TupleValueExpression>();
    return row.DeriveValue(codegen, tve_expr.GetAttributeRef());
  }
};

}  // namespace codegen
}  // namespace peloton