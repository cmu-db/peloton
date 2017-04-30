//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregation_translator.h
//
// Identification: src/include/codegen/aggregation_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression_translator.h"

namespace peloton {

namespace expression {
class AggregateExpression;
}  // namespace expression

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for expressions that access a specific attribute produced
// by the aggregate function in a tuple
//===----------------------------------------------------------------------===//
class AggregationTranslator : public ExpressionTranslator {
 public:
  // Constructor
  AggregationTranslator(const expression::AggregateExpression &agg_expr,
                       CompilationContext &context);

  // Return the attribute from the row
  Value DeriveValue(CodeGen &codegen, RowBatch::Row &row) const override;
};

}  // namespace codegen
}  // namespace peloton