//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_value_translator.cpp
//
// Identification: src/codegen/tuple_value_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/tuple_value_translator.h"

#include "expression/tuple_value_expression.h"
#include "codegen/parameter.h"
#include "codegen/compilation_context.h"
#include "codegen/value_factory_proxy.h"
#include "codegen/value_proxy.h"

namespace peloton {
namespace codegen {

// Constructor
TupleValueTranslator::TupleValueTranslator(
    const expression::TupleValueExpression &tve_expr,
    CompilationContext &context)
    : ExpressionTranslator(tve_expr, context), ctx_(context){
  PL_ASSERT(tve_expr.GetAttributeRef() != nullptr);
  offset_ = context.StoreParam(Parameter::GetTupleValParamInstance());
}

codegen::Value TupleValueTranslator::DeriveValue(CodeGen &codegen,
                                                 RowBatch::Row &row) const {
  const auto &tve_expr = GetExpressionAs<expression::TupleValueExpression>();
  return row.DeriveValue(codegen, tve_expr.GetAttributeRef());
}

// Produce the value that is the result of codegening the expression
codegen::Value TupleValueTranslator::DeriveTypeValue(CodeGen &codegen,
                                                 RowBatch::Row &row) const {
  auto value = DeriveValue(codegen, row);
  type::Type::TypeId type_id_ = value.GetType();

  // Prepare parameters for calling value factory
  std::vector<llvm::Value *> args =
        {ctx_.GetValuesPtr(), codegen.Const64(offset_), value.GetValue()};

  // Generate type::Value in llvm form
  switch (type_id_) {
    case type::Type::TINYINT:
      codegen.CallFunc(
              ValueFactoryProxy::_GetTinyIntValue::GetFunction(codegen),
              args);
      break;
    case type::Type::SMALLINT:
      codegen.CallFunc(
              ValueFactoryProxy::_GetSmallIntValue::GetFunction(codegen),
              args);
      break;
    case type::Type::INTEGER:
      codegen.CallFunc(
              ValueFactoryProxy::_GetIntegerValue::GetFunction(codegen),
              args);
      break;
    case type::Type::BIGINT:
      codegen.CallFunc(
              ValueFactoryProxy::_GetBigIntValue::GetFunction(codegen),
              args);
      break;
    case type::Type::DECIMAL:
      codegen.CallFunc(
              ValueFactoryProxy::_GetDecimalValue::GetFunction(codegen),
              args);
      break;
    case type::Type::TIMESTAMP:
      codegen.CallFunc(
              ValueFactoryProxy::_GetTimestampValue::GetFunction(codegen),
              args);
      break;
    case type::Type::DATE:
      codegen.CallFunc(
              ValueFactoryProxy::_GetDateValue::GetFunction(codegen),
              args);
      break;
    case type::Type::VARCHAR:
      codegen.CallFunc(
              ValueFactoryProxy::_GetVarcharValue::GetFunction(codegen),
              {ctx_.GetValuesPtr(), codegen.Const64(offset_),
               value.GetValue(), value.GetLength()});
      break;
    case type::Type::VARBINARY:
      codegen.CallFunc(
              ValueFactoryProxy::_GetVarbinaryValue::GetFunction(codegen),
              {ctx_.GetValuesPtr(), codegen.Const64(offset_),
               value.GetValue(), value.GetLength()});
      break;
    default: {
      throw Exception{"Unsupported column type: " +
                      TypeIdToString(type_id_)};
    }
  }

  llvm::Value *ret = codegen.CallFunc(
      ValueProxy::_GetValue::GetFunction(codegen),
      {ctx_.GetValuesPtr(), codegen.Const64(offset_)});

  return codegen::Value{value.GetType(), ret, nullptr};
}

}  // namespace codegen
}  // namespace peloton