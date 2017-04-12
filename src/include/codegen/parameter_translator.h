//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_translator.h
//
// Identification: src/include/codegen/parameter_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression_translator.h"
#include "expression/constant_value_expression.h"
#include "codegen/compilation_context.h"

namespace peloton {

namespace expression {
  class AbstractExpression;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// A param expression translator just produces the LLVM value version of the
// constant/param value within.
//===----------------------------------------------------------------------===//
class ParameterTranslator : public ExpressionTranslator {
 public:
  ParameterTranslator(const expression::AbstractExpression &exp,
                      CompilationContext &ctx);

  // Produce the value that is the result of codegen-ing the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;

 private:
  type::Type::TypeId typeId_;
  uint32_t offset_;
  CompilationContext &ctx_;
};

}  // namespace codegen
}  // namespace peloton