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
  NegationTranslator(const expression::OperatorUnaryMinusExpression &unary_minus_expression,
                     CompilationContext &ctx);

  Value DeriveValue(CodeGen &codegen, RowBatch::Row &row) const override;
  codegen::Value DeriveTypeValue(CodeGen &codegen, RowBatch::Row &row) const;

 private:
  uint32_t offset_;
  CompilationContext &ctx_;
};

}  // namespace codegen
}  // namespace peloton