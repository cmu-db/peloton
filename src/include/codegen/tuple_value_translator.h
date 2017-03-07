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

#include "codegen/consumer_context.h"
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
  TupleValueTranslator(const expression::TupleValueExpression &tve_exp,
                       CompilationContext &context)
      : ExpressionTranslator(context), tve_exp_(tve_exp) {}

  // Produce the value that is the result of codegening the expression
  codegen::Value DeriveValue(ConsumerContext &context,
                             RowBatch::Row &row) const override {
    PL_ASSERT(tve_exp_.GetAttributeRef() != nullptr);
    return row.GetAttribute(context.GetCodeGen(), tve_exp_.GetAttributeRef());
  }

 private:
  // The expression
  const expression::TupleValueExpression &tve_exp_;
};

}  // namespace codegen
}  // namespace peloton