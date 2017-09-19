//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_translator.cpp
//
// Identification: src/codegen/expression/parameter_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/parameter_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/proxy/query_parameters_proxy.h"
#include "codegen/type/sql_type.h"
#include "expression/parameter_value_expression.h"

namespace peloton {
namespace codegen {

// Constructor
ParameterTranslator::ParameterTranslator(
    const expression::ParameterValueExpression &exp, CompilationContext &ctx)
    : ExpressionTranslator(exp, ctx) {
  parameter_index_ = ctx.GetParameterIdx(&exp);
}

// Return an LLVM value for the constant: run-time value
codegen::Value ParameterTranslator::DeriveValue(
    CodeGen &codegen, UNUSED_ATTRIBUTE RowBatch::Row &row) const {
  llvm::Value *val = nullptr;
  llvm::Value *len = nullptr;
  std::vector<llvm::Value *> val_args = {context_.GetQueryParametersPtr(),
                                         codegen.Const32(parameter_index_)};
  auto type_id =
    GetExpressionAs<expression::ParameterValueExpression>().GetValueType();
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
      throw Exception{"Unknown parameter value type " +
                      TypeIdToString(type_id)};
    }
  }
  
  return codegen::Value{type::SqlType::LookupType(type_id), val, len, nullptr};
}

}  // namespace codegen
}  // namespace peloton
