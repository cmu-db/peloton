//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// negation_translator.cpp
//
// Identification: src/codegen/negation_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/negation_translator.h"

#include "codegen/compilation_context.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace codegen {

// Constructor
NegationTranslator::NegationTranslator(
    const expression::OperatorUnaryMinusExpression &expr,
    CompilationContext &ctx)
    : ExpressionTranslator(ctx), expr_(expr) {
  PL_ASSERT(expr_.GetChildrenSize() == 1);
  ctx.Prepare(*expr_.GetChild(0));
}

Value NegationTranslator::DeriveValue(ConsumerContext &context,
                                      RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();
  codegen::Value child_value = context.DeriveValue(*expr_.GetChild(0), row);
  codegen::Value zero{type::Type::TypeId::INTEGER, codegen.Const32(0)};
  return zero.Sub(codegen, child_value);
}

}  // namespace codegen
}  // namespace peloton