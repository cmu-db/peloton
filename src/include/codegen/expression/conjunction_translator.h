//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// conjunction_translator.h
//
// Identification: src/include/codegen/expression/conjunction_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/expression_translator.h"

namespace peloton {

namespace expression {
class ConjunctionExpression;
}  // namespace expression

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for conjunction expressions
//===----------------------------------------------------------------------===//
class ConjunctionTranslator : public ExpressionTranslator {
 public:
  // Constructor
  ConjunctionTranslator(const expression::ConjunctionExpression &conjunction,
                        CompilationContext &context);

  // Produce the value that is the result of codegening the expression
  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
};

}  // namespace codegen
}  // namespace peloton