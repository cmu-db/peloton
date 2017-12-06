//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// null_check_translator.h
//
// Identification: src/include/codegen/expression/null_check_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/expression/expression_translator.h"

namespace peloton {

namespace expression {
class OperatorExpression;
}  // namespace expression

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator of check_null expressions.
//===----------------------------------------------------------------------===//
class NullCheckTranslator : public ExpressionTranslator {
 public:
  // Constructor
  NullCheckTranslator(const expression::OperatorExpression &null_check,
                      CompilationContext &context);

  // Produce the value that is the result of codegening the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
};

}  // namespace codegen
}  // namespace peloton