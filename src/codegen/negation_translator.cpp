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

#include <include/codegen/value_proxy.h>
#include <include/codegen/value_peeker_proxy.h>
#include <include/codegen/value_factory_proxy.h>
#include "codegen/negation_translator.h"

#include "expression/operator_expression.h"
#include "codegen/compilation_context.h"
#include "codegen/parameter.h"

namespace peloton {
namespace codegen {

// Constructor
NegationTranslator::NegationTranslator(
    const expression::OperatorUnaryMinusExpression &unary_minus_expression,
    CompilationContext &ctx)
    : ExpressionTranslator(unary_minus_expression, ctx), ctx_(ctx) {
  PL_ASSERT(unary_minus_expression.GetChildrenSize() == 1);
  offset_ = ctx_.StoreParam(Parameter::GetTupleValParamInstance());
  ctx_.StoreParam(Parameter::GetTupleValParamInstance());
}

Value NegationTranslator::DeriveValue(CodeGen &codegen,
                                          RowBatch::Row &row) const {
  return DeriveTypeValue(codegen, row);
}

Value NegationTranslator::DeriveTypeValue(CodeGen &codegen,
                                      RowBatch::Row &row) const {
  const auto &negation_expr =
      GetExpressionAs<expression::OperatorUnaryMinusExpression>();

  codegen::Value child_value =
      row.DeriveTypeValue(codegen, *negation_expr.GetChild(0));

  codegen.CallFunc(
          ValueFactoryProxy::_GetIntegerValue::GetFunction(codegen),
          {ctx_.GetValuesPtr(), codegen.Const64(offset_), codegen.Const32(0)});

  llvm::Value *zero = codegen.CallFunc(
          ValueProxy::_GetValue::GetFunction(codegen),
          {ctx_.GetValuesPtr(), codegen.Const64(offset_)});

  codegen.CallFunc(
        ValueProxy::_OpMinus::GetFunction(codegen),
        {zero, child_value.GetValue(),
         ctx_.GetValuesPtr(), codegen.Const64(offset_ + 1)});

  llvm::Value *result = codegen.CallFunc(
        ValueProxy::_GetValue::GetFunction(codegen),
        {ctx_.GetValuesPtr(), codegen.Const64(offset_ + 1)});

  return codegen::Value{child_value.GetType(), result};

}

}  // namespace codegen
}  // namespace peloton