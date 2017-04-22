//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.cpp
//
// Identification: src/codegen/constant_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/constant_translator.h"

#include "type/value_peeker.h"
#include "codegen/value_peeker_proxy.h"
#include "codegen/value_proxy.h"
#include "expression/parameter_value_expression.h"

namespace peloton {
namespace codegen {

// Constructor
ConstantTranslator::ConstantTranslator(
    const expression::AbstractExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {
  switch (exp.GetExpressionType()) {
    case ExpressionType::VALUE_CONSTANT: {
      const type::Value &constant =
            GetExpressionAs<expression::ConstantValueExpression>().GetValue();
      offset_ = ctx.StoreParam(Parameter::GetConstValParamInstance(constant));
      break;
    }
    case ExpressionType::VALUE_PARAMETER: {
      int param_idx =
            GetExpressionAs<expression::ParameterValueExpression>().GetValueIdx();
      offset_ = ctx.StoreParam(Parameter::GetParamValParamInstance(param_idx));
      break;
    }
    default: {
      throw Exception{"Illegal instantiation for constant translator. Expression type: " +
                      ExpressionTypeToString(exp.GetExpressionType())};
    }
  }
}

// Return an LLVM value for our constant (i.e., a compile-time constant)
codegen::Value ConstantTranslator::DeriveValue(CodeGen &codegen,
                                               RowBatch::Row &) const {
  std::vector<llvm::Value *> args =
          {GetValuesPtr(), codegen.Const64(offset_)};

  llvm::Value *value = codegen.CallFunc(
          ValueProxy::_GetValue::GetFunction(codegen),
          args);

  // Convert the value into an LLVM compile-time constant
  llvm::Value *val = nullptr;
  llvm::Value *len = nullptr;
  auto type_id = GetValueType();
  switch (type_id) {
    case type::Type::TypeId::PARAMETER_OFFSET: {
      break;
    }
    case type::Type::TypeId::TINYINT: {
      val = codegen.CallFunc(
              ValuePeekerProxy::_PeekTinyInt::GetFunction(codegen),
              {value});
      break;
    }
    case type::Type::TypeId::SMALLINT: {
      val = codegen.CallFunc(
              ValuePeekerProxy::_PeekSmallInt::GetFunction(codegen),
              {value});
      break;
    }
    case type::Type::TypeId::INTEGER: {
      val = codegen.CallFunc(
              ValuePeekerProxy::_PeekInteger::GetFunction(codegen),
              {value});
      break;
    }
    case type::Type::TypeId::BIGINT: {
      val = codegen.CallFunc(
              ValuePeekerProxy::_PeekBigInt::GetFunction(codegen),
              {value});
      break;
    }
    case type::Type::TypeId::DECIMAL: {
      val = codegen.CallFunc(
              ValuePeekerProxy::_PeekDouble::GetFunction(codegen),
              {value});
      break;
    }
    case type::Type::TypeId::DATE: {
      val = codegen.CallFunc(
              ValuePeekerProxy::_PeekDate::GetFunction(codegen),
              {value});
      break;
    }
    case type::Type::TypeId::TIMESTAMP: {
      val = codegen.CallFunc(
              ValuePeekerProxy::_PeekTimestamp::GetFunction(codegen),
              {value});
      break;
    }
    case type::Type::TypeId::VARCHAR: {
      val = codegen.CallFunc(
              ValuePeekerProxy::_PeekVarcharVal::GetFunction(codegen),
              {value});
      len = codegen.CallFunc(
              ValuePeekerProxy::_PeekVarcharLen::GetFunction(codegen),
              {value});
      break;
    }
    default: {
      throw Exception{"Unknown constant value type " +
                      TypeIdToString(type_id)};
    }
  }
  return codegen::Value{type_id, val, len};
}

}  // namespace codegen
}  // namespace peloton