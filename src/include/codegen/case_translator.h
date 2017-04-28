//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// case_translator.h
//
// Identification: src/include/codegen/case_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/expression_translator.h"
//#include "expression/case_expression.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for case expressions
// TODO: Uncomment when we have case ...
//===----------------------------------------------------------------------===//
/*
class CaseTranslator : public ExpressionTranslator {
 public:
  CaseTranslator(const expression::CaseExpression &case_expression,
                 CompilationContext &context);

  codegen::Value DeriveValue(ConsumerContext &context,
                             RowBatch::Row &row) const override;

 private:
  // The case expression
  const expression::CaseExpression &case_expression_;
};
*/

}  // namespace codegen
}  // namespace peloton