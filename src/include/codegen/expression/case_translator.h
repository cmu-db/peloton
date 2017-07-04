//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// case_translator.h
//
// Identification: src/include/codegen/expression/case_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression_translator.h"
#include "expression/case_expression.h"

namespace peloton {

namespace codegen {

class CaseTranslator : public ExpressionTranslator {
 public:
  CaseTranslator(const expression::CaseExpression &expression,
                 CompilationContext &context);

  codegen::Value DeriveValue(CodeGen &codegen,
                             RowBatch::Row &row) const override;
};

}  // namespace codegen
}  // namespace peloton
