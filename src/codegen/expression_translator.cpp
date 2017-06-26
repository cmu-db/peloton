//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_translator.cpp
//
// Identification: src/codegen/expression_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression_translator.h"

#include "codegen/compilation_context.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"

namespace peloton {
namespace codegen {

ExpressionTranslator::ExpressionTranslator(
    const expression::AbstractExpression &expression, CompilationContext &ctx)
    : expression_(expression) {
  if (expression::ExpressionUtil::IsAggregateExpression(expression.GetExpressionType()))
    return;
  for (uint32_t i = 0; i < expression_.GetChildrenSize(); i++) {
    ctx.Prepare(*expression_.GetChild(i));
  }
}

}  // namespace codegen
}  // namespace peloton