//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_translator.h
//
// Identification: src/include/codegen/expression/function_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "codegen/expression/expression_translator.h"
#include <unordered_set>

namespace peloton {

namespace expression {
class FunctionExpression;
}

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for function expression
//===----------------------------------------------------------------------===//
class FunctionTranslator : public ExpressionTranslator {
 public:
  // Constructor
  FunctionTranslator(const expression::FunctionExpression &func_expr,
                     CompilationContext &context);

  // Return the result of the function call
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
 private:
  CompilationContext &context_;
};

}  // namespace codegen
}  // namespace peloton
