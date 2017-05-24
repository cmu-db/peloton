//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.h
//
// Identification: src/include/codegen/constant_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression_translator.h"
#include "expression/constant_value_expression.h"

namespace peloton {

namespace expression {
class ConstantValueExpression;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// A const expression translator just produces the LLVM value version of the
// constant value within.
//===----------------------------------------------------------------------===//
class ConstantTranslator : public ExpressionTranslator {
 public:
  ConstantTranslator(const expression::AbstractExpression &exp,
                     CompilationContext &ctx);

  // Produce the value that is the result of codegen-ing the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;

  codegen::Value DeriveTypeValue(CodeGen &codegen,
                                 RowBatch::Row &row) const;

 private:
  uint32_t offset_;
};

}  // namespace codegen
}  // namespace peloton