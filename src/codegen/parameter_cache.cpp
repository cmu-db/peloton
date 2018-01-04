//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_cache.cpp
//
// Identification: src/include/codegen/parameter_cache.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen.h"
#include "codegen/parameter_cache.h"
#include "codegen/updateable_storage.h"
#include "codegen/value.h"
#include "codegen/proxy/query_parameters_proxy.h"
#include "codegen/type/sql_type.h"
#include "codegen/type/type.h"

namespace peloton {
namespace codegen {

void ParameterCache::Populate(CodeGen &codegen,
    llvm::Value *query_parameters_ptr) {
  auto &parameters = parameters_map_.GetParameters();
  for (uint32_t i = 0; i < parameters.size(); i++) {
    auto &parameter = parameters[i];
    auto type_id = parameter.GetValueType();
    auto is_nullable = parameter.IsNullable();
    auto val = DeriveParameterValue(codegen, query_parameters_ptr, i,
                                    type_id, is_nullable);
    values_.push_back(val);
  }
}

codegen::Value ParameterCache::GetValue(uint32_t index) const {
  return values_[index];
}

void ParameterCache::Reset() {
  values_.clear();
}

codegen::Value ParameterCache::DeriveParameterValue(CodeGen &codegen,
    llvm::Value *query_parameters_ptr, uint32_t index,
    peloton::type::TypeId type_id, bool is_nullable) {
  llvm::Value *val = nullptr, *len = nullptr;
  std::vector<llvm::Value *> args = {query_parameters_ptr,
                                     codegen.Const32(index)};
  switch (type_id) {
    case peloton::type::TypeId::BOOLEAN: {
      val = codegen.Call(QueryParametersProxy::GetBoolean, args);
      break;
    }
    case peloton::type::TypeId::TINYINT: {
      val = codegen.Call(QueryParametersProxy::GetTinyInt, args);
      break;
    }
    case peloton::type::TypeId::SMALLINT: {
      val = codegen.Call(QueryParametersProxy::GetSmallInt, args);
      break;
    }
    case peloton::type::TypeId::INTEGER: {
      val = codegen.Call(QueryParametersProxy::GetInteger, args);
      break;
    }
    case peloton::type::TypeId::BIGINT: {
      val = codegen.Call(QueryParametersProxy::GetBigInt, args);
      break;
    }
    case peloton::type::TypeId::DECIMAL: {
      val = codegen.Call(QueryParametersProxy::GetDouble, args);
      break;
    }
    case peloton::type::TypeId::DATE: {
      val = codegen.Call(QueryParametersProxy::GetDate, args);
      break;
    }
    case peloton::type::TypeId::TIMESTAMP: {
      val = codegen.Call(QueryParametersProxy::GetTimestamp, args);
      break;
    }
    case peloton::type::TypeId::VARCHAR: {
      val = codegen.Call(QueryParametersProxy::GetVarcharVal, args);
      len = codegen.Call(QueryParametersProxy::GetVarcharLen, args);
      break;
    }
    case peloton::type::TypeId::VARBINARY: {
      val = codegen.Call(QueryParametersProxy::GetVarbinaryVal, args);
      len = codegen.Call(QueryParametersProxy::GetVarbinaryLen, args);
      break;
    }
    default: {
      throw Exception{"Unknown parameter storage value type " +
                      TypeIdToString(type_id)};
    }
  }
  llvm::Value *is_null = nullptr;
  if (is_nullable) {
    is_null = codegen.Call(QueryParametersProxy::IsNull, args);
  }
  return Value{type::Type{type_id, is_nullable}, val, len, is_null};
}

}  // namespace codegen
}  // namespace peloton
