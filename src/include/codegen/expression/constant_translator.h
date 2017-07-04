//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.h
//
// Identification: src/include/codegen/expression/constant_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression_translator.h"

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
  ConstantTranslator(const expression::ConstantValueExpression &exp,
                     CompilationContext &ctx);

  // Produce the value that is the result of codegen-ing the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
};

}  // namespace codegen
}  // namespace peloton