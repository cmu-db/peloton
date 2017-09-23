//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_translator.cpp
//
// Identification: src/codegen/expression/constant_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/constant_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/proxy/query_parameters_proxy.h"
#include "codegen/type/sql_type.h"
#include "expression/constant_value_expression.h"

namespace peloton {
namespace codegen {

// Constructor
ConstantTranslator::ConstantTranslator(
    const expression::ConstantValueExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {
  parameter_index_ = ctx.GetParameterIdx(&exp);
}

// Return an LLVM value for our constant: values passed over at run time
codegen::Value ConstantTranslator::DeriveValue(
    CodeGen &codegen, UNUSED_ATTRIBUTE RowBatch::Row &row) const {
  llvm::Value *val = nullptr;
  llvm::Value *len = nullptr;
  std::vector<llvm::Value *> val_args = {context_.GetQueryParametersPtr(),
                                         codegen.Const32(parameter_index_)};
  const peloton::type::Value &constant =
      GetExpressionAs<expression::ConstantValueExpression>().GetValue();
  auto type_id = constant.GetTypeId();
  switch (type_id) {
    case peloton::type::TypeId::TINYINT: {
      val = codegen.Call(QueryParametersProxy::GetTinyInt, val_args);
      break;
    }
    case peloton::type::TypeId::SMALLINT: {
      val = codegen.Call(QueryParametersProxy::GetSmallInt, val_args);
      break;
    }
    case peloton::type::TypeId::INTEGER: {
      val = codegen.Call(QueryParametersProxy::GetInteger, val_args);
      break;
    }
    case peloton::type::TypeId::BIGINT: {
      val = codegen.Call(QueryParametersProxy::GetBigInt, val_args);
      break;
    }
    case peloton::type::TypeId::DECIMAL: {
      val = codegen.Call(QueryParametersProxy::GetDouble, val_args);
      break;
    }
    case peloton::type::TypeId::DATE: {
      val = codegen.Call(QueryParametersProxy::GetDate, val_args);
      break;
    }
    case peloton::type::TypeId::TIMESTAMP: {
      val = codegen.Call(QueryParametersProxy::GetTimestamp, val_args);
      break;
    }
    case peloton::type::TypeId::VARCHAR: {
      val = codegen.Call(QueryParametersProxy::GetVarcharVal, val_args);
      len = codegen.Call(QueryParametersProxy::GetVarcharLen, val_args);
      break;
    }
    default: {
      throw Exception{"Unknown constant value type " + TypeIdToString(type_id)};
    }
  }
  return codegen::Value{type::SqlType::LookupType(type_id), val, len, nullptr};
}

}  // namespace codegen
}  // namespace peloton
