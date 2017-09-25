//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter.h
//
// Identification: src/include/expression/parameter.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace expression {

// We have one Parameter class both for CONSTANT and PARAMETER expressions.
// This is to simplify the implementation over having separate inherited classes
class Parameter {
 public:
  enum class Type { CONSTANT = 0, PARAMETER = 1 };

  static Parameter CreateConstParameter(type::Value value) {
    return Parameter{value.GetTypeId(), value};
  }

  static Parameter CreateParamParameter(int32_t param_idx) {
    return Parameter{param_idx};
  }

  Type GetType() { return type_; }

  peloton::type::TypeId GetValueType() { return type_id_; }

  type::Value &GetValue() {
    PL_ASSERT(type_==Type::CONSTANT);
    return value_;
  }

  uint32_t GetParamIdx() {
    PL_ASSERT(type_==Type::PARAMETER);
    return param_idx_;
  }

 private:
  Parameter(peloton::type::TypeId type_id, type::Value value)
      : type_(Type::CONSTANT), type_id_(type_id), value_(value) {}

  Parameter(int32_t param_idx)
      : type_(Type::PARAMETER), type_id_(type::TypeId::INVALID),
        param_idx_(param_idx) {}

 private:
  Type type_;
  peloton::type::TypeId type_id_;

  // Value for ConstantValueExpression
  type::Value value_;

  // Index for ParameterValueExpression
  int32_t param_idx_;
};

}  // namespace codegen
}  // namespace peloton
