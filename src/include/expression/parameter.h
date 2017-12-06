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

// This class keeps a parameter's meta information such as type, etc.
class Parameter {
 public:
  enum class Type { CONSTANT = 0, PARAMETER = 1 };

  // Create Constant Parameter object
  static Parameter CreateConstParameter(type::TypeId type_id,
                                        bool is_nullable) {
    return Parameter{Type::CONSTANT, type_id, is_nullable};
  }

  // Create Parameter Parameter object
  static Parameter CreateParamParameter(type::TypeId type_id,
                                        bool is_nullable) {
    return Parameter{Type::PARAMETER, type_id, is_nullable};
  }

  // Get type of the parameter
  Type GetType() const { return type_; }

  // Get value type of the parameter
  peloton::type::TypeId GetValueType() const { return type_id_; }

  // Get its nullabilityy
  bool IsNullable() const { return is_nullable_; }

 private:
  Parameter(Type type, peloton::type::TypeId type_id, bool is_nullable)
      : type_(type), type_id_(type_id), is_nullable_(is_nullable) {}

 private:
  // Type of the parameter
  Type type_;

  // Type Id of the value
  peloton::type::TypeId type_id_;

  // Boolean to show if the value is nullable
  bool is_nullable_;
};

}  // namespace codegen
}  // namespace peloton
