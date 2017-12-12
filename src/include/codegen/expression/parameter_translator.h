//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_translator.h
//
// Identification: src/include/codegen/expression/parameter_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression/expression_translator.h"

namespace peloton {

namespace expression {
class ParameterValueExpression;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// A parameter expression translator just produces the LLVM value version of the
// constant value within.
//===----------------------------------------------------------------------===//
class ParameterTranslator : public ExpressionTranslator {
 public:
  ParameterTranslator(const expression::ParameterValueExpression &exp,
                      CompilationContext &ctx);

  // Produce the value that is the result of codegen-ing the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
 private:
  size_t index_;
};

}  // namespace codegen
}  // namespace peloton
