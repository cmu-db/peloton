//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_storage.h
//
// Identification: src/include/codegen/parameter_storage.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/type/sql_type.h"
#include "codegen/updateable_storage.h"
#include "codegen/value.h"
#include "codegen/proxy/query_parameters_proxy.h"
#include "expression/parameter.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_peeker.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A storage area where slots can be updated.
//===----------------------------------------------------------------------===//
class ParameterStorage {
 public:
  // Constructor
  ParameterStorage() : space_ptr_(nullptr) {}

  // Build up the parameter storage and load up the values
  llvm::Type *Setup(CodeGen &codegen,
                    std::vector<expression::Parameter> &parameters) {
    parameters_ = &parameters;
    // Build the storage with type information
    // TODO Check whether we need to support nullability
    //      We have implemented the rest to support nullability
    for (auto &parameter: parameters)
      storage_.AddType(codegen::type::Type{parameter.GetValueType(),
                                           false});
    return storage_.Finalize(codegen);
  }

  // Set the parameter values
  void SetValues(CodeGen &codegen, llvm::Value *query_parameters_ptr,
                 llvm::Value *space_ptr) {
    // Find out the storage size and allocate the storage area
    space_ptr_ = space_ptr;

    // Set the values to the storage
    UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, space_ptr_};
    for (uint32_t i = 0; i < parameters_->size(); i++) {
      auto &parameter = parameters_->at(i);
      auto val = DeriveParameterValue(codegen, parameter, query_parameters_ptr,
                                      i);
      if (null_bitmap.IsNullable(i))
        storage_.SetValue(codegen, space_ptr_, i, val, null_bitmap);
      else
        storage_.SetValueSkipNull(codegen, space_ptr_, i, val);
    }
    null_bitmap.WriteBack(codegen);
  }

  codegen::Value GetValue(CodeGen &codegen, uint32_t index) const {
    PL_ASSERT(space_ptr_);
    UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, space_ptr_};
    if (null_bitmap.IsNullable(index))
      return storage_.GetValue(codegen, space_ptr_, index, null_bitmap);
    else
      return storage_.GetValueSkipNull(codegen, space_ptr_, index);
  }

 private:
  codegen::Value DeriveParameterValue(CodeGen &codegen,
      expression::Parameter &parameter, llvm::Value *query_parameters_ptr,
      uint32_t index) {
    llvm::Value *val = nullptr, *len = nullptr;
    std::vector<llvm::Value *> args = {query_parameters_ptr,
                                       codegen.Const32(index)};
    auto type_id = parameter.GetValueType();
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
      default: {
        throw Exception{"Unknown constant value type " +
                        TypeIdToString(type_id)};
      }
    }
    return codegen::Value{type::SqlType::LookupType(type_id), val, len,
                          nullptr};
  }

 private:
  // Storage format
  UpdateableStorage storage_;

  // Storage space
  llvm::Value *space_ptr_;

  // Parameters' information
  std::vector<expression::Parameter> *parameters_;
};

}  // namespace codegen
}  // namespace peloton
