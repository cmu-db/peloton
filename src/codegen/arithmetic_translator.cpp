//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// arithmetic_translator.cpp
//
// Identification: src/codegen/arithmetic_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/arithmetic_translator.h"

#include "codegen/value_proxy.h"
#include "expression/operator_expression.h"

namespace peloton {
namespace codegen {

// Constructor
ArithmeticTranslator::ArithmeticTranslator(
    const expression::OperatorExpression &arithmetic,
    CompilationContext &context)
    : ExpressionTranslator(arithmetic, context), ctx_(context) {
  PL_ASSERT(arithmetic.GetChildrenSize() == 2);
  offset_ = context.StoreParam(Parameter::GetTupleValParamInstance());
}

codegen::Value ArithmeticTranslator::DeriveValue(CodeGen &codegen,
                                                 RowBatch::Row &row) const {
  return DeriveTypeValue(codegen, row);
}

// Produce the value that is the result of codegening the expression
codegen::Value ArithmeticTranslator::DeriveTypeValue(CodeGen &codegen,
                                                 RowBatch::Row &row) const {
  const auto &arithmetic = GetExpressionAs<expression::OperatorExpression>();
  codegen::Value left = row.DeriveTypeValue(codegen, *arithmetic.GetChild(0));
  codegen::Value right = row.DeriveTypeValue(codegen, *arithmetic.GetChild(1));

  // Prepare parameters for calling value factory
  std::vector<llvm::Value *> args =
            {left.GetValue(), right.GetValue(),
             ctx_.GetValuesPtr(), codegen.Const64(offset_)};

  switch (arithmetic.GetExpressionType()) {
    case ExpressionType::OPERATOR_PLUS:
      codegen.CallFunc(
            ValueProxy::_OpPlus::GetFunction(codegen),
            args);
      break;
    case ExpressionType::OPERATOR_MINUS:
      codegen.CallFunc(
            ValueProxy::_OpMinus::GetFunction(codegen),
            args);
      break;
    case ExpressionType::OPERATOR_MULTIPLY:
      codegen.CallFunc(
            ValueProxy::_OpMultiply::GetFunction(codegen),
            args);
      break;
    case ExpressionType::OPERATOR_DIVIDE:
      codegen.CallFunc(
            ValueProxy::_OpDevide::GetFunction(codegen),
            args);
      break;
    case ExpressionType::OPERATOR_MOD:
      codegen.CallFunc(
            ValueProxy::_OpMod::GetFunction(codegen),
            args);
      break;
    default: {
      throw Exception(
          "Arithmetic expression has invalid type for translation: " +
          ExpressionTypeToString(arithmetic.GetExpressionType()));
    }
  }

  llvm::Value *ret = codegen.CallFunc(
        ValueProxy::_GetValue::GetFunction(codegen),
        {ctx_.GetValuesPtr(), codegen.Const64(offset_)});

  return codegen::Value{left.GetType(), ret, nullptr};
}

}  // namespace codegen
}  // namespace peloton