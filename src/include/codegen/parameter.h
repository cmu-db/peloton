//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter.h
//
// Identification: src/include/codegen/parameter.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/code_context.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A parameter structure is determined at runtime
//===----------------------------------------------------------------------===//
class Parameter {
 public:
  enum class ParamType {
    Const = 0,
    Param = 1
  };

  static Parameter GetConstValParamInstance(type::Value value) {
    return Parameter{ParamType::Const, value, 0};
  }

  static Parameter GetParamValParamInstance(int param_idx) {
    return Parameter{ParamType::Param,
                     type::ValueFactory::GetBooleanValue(false),
                     param_idx};
  }


  type::Value GetValue() {
    return value_;
  }

  ParamType GetType() {
    return type_;
  }

  int GetParamIdx() {
    return param_idx_;
  }

 private:
  Parameter(ParamType type,
            type::Value value, int param_idx)
          : type_(type),
            value_(value), param_idx_(param_idx) {}

 private:
  ParamType type_;

  // Field for CVE, trivial for PVE
  type::Value value_;

  // Field for PVE, trivial for CVE
  int param_idx_;
};

}  // namespace codegen
}  // namespace peloton