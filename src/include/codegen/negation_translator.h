//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// negation_translator.h
//
// Identification: src/include/codegen/negation_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression_translator.h"

namespace peloton {

namespace expression {
class OperatorUnaryMinusExpression;
}  // namespace expression

namespace codegen {

class NegationTranslator : public ExpressionTranslator {
 public:
  // Constructor
  NegationTranslator(const expression::OperatorUnaryMinusExpression &expr,
                     CompilationContext &ctx);

  Value DeriveValue(ConsumerContext &context,
                    RowBatch::Row &row) const override;

 private:
  const expression::OperatorUnaryMinusExpression &expr_;
};

}  // namespace codegen
}  // namespace peloton