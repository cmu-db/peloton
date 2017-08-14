#pragma once

#include "codegen/expression/expression_translator.h"

namespace peloton {

namespace expression {
class FunctionExpression;
}  // namespace expression

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
};

}  // namespace codegen
}  // namespace peloton
