//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// comparison_translator.h
//
// Identification: src/include/codegen/expression/comparison_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/expression_translator.h"

namespace peloton {

namespace expression {
class ComparisonExpression;
}  // namespace expression

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator of comparison expressions.
//===----------------------------------------------------------------------===//
class ComparisonTranslator : public ExpressionTranslator {
 public:
  // Constructor
  ComparisonTranslator(const expression::ComparisonExpression &comparison,
                       CompilationContext &context);

  // Produce the result of performing the comparison of left and right values
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
};

}  // namespace codegen
}  // namespace peloton