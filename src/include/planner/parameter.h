//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter.h
//
// Identification: src/include/planner/parameter.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace planner {

// We have Parameter class both for CONSTANT and PARAMETER. Later, we could
// have some parent/child relationships, but we simplify the implementation for // having them together in one class
class Parameter {
 public:
  enum class Type { CONSTANT = 0, PARAMETER = 1 };

  static Parameter CreateConstant(type::Value value) {
    return Parameter{Type::CONSTANT, value.GetTypeId(), value, 0};
  }

  static Parameter CreateParameter(peloton::type::TypeId type_id,
      int32_t param_idx) {
    return Parameter{Type::PARAMETER, type_id,
                     type::ValueFactory::GetBooleanValue(false), param_idx};
  }

  Type GetType() { return type_; }

  peloton::type::TypeId GetValueType() { return type_id_; }

  // For CONSTANT
  type::Value GetValue() { return value_; }

  // For PARAMTER
  uint32_t GetParamIdx() { return param_idx_; }

 private:
  Parameter(Type type, peloton::type::TypeId type_id,type::Value value,
            int32_t param_idx)
      : type_(type), type_id_(type_id), value_(value), param_idx_(param_idx) {}

 private:
  // Common
  Type type_;

  peloton::type::TypeId type_id_;

  // Actual value for ConstantValueExpression
  type::Value value_;

  // Index value for ParameterValueExpression
  int32_t param_idx_;
};

}  // namespace codegen
}  // namespace peloton
