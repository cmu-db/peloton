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

class Parameter {
 public:
  enum class Type { CONSTANT = 0, PARAMETER = 1 };

  // Create Constant Parameter object
  static Parameter CreateConstParameter(type::Value value,
                                        bool is_nullable = true) {
    return Parameter{value.GetTypeId(), value, is_nullable};
  }

  // Create Parameter Parameter object
  static Parameter CreateParamParameter(int32_t param_idx,
                                        bool is_nullable = true) {
    return Parameter{param_idx, is_nullable};
  }

  // Get type of the parameter
  Type GetType() { return type_; }

  // Get value type of the parameter
  peloton::type::TypeId GetValueType() { return type_id_; }

  // Get its nullabilityy
  bool IsNullable() { return is_nullable_; }

  // Get value
  type::Value &GetValue() {
    PL_ASSERT(type_==Type::CONSTANT);
    return value_;
  }

  // Get paramer index
  uint32_t GetParamIdx() {
    PL_ASSERT(type_ == Type::PARAMETER);
    return param_idx_;
  }

 private:
  Parameter(peloton::type::TypeId type_id, type::Value value, bool is_nullable)
      : type_(Type::CONSTANT), type_id_(type_id), value_(value),
        is_nullable_(is_nullable) {}

  Parameter(int32_t param_idx, bool is_nullable)
      : type_(Type::PARAMETER), type_id_(type::TypeId::INVALID),
        param_idx_(param_idx), is_nullable_(is_nullable) {}

 private:
  // Type of the parameter
  Type type_;

  // Type Id of the value
  peloton::type::TypeId type_id_;

  // Value for ConstantValueExpression
  type::Value value_;

  // Index for ParameterValueExpression
  int32_t param_idx_;

  // Boolean to show if the value is nullable
  bool is_nullable_;
};

}  // namespace codegen
}  // namespace peloton
