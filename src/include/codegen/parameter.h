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

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A parameter structure is determined at runtime
//===----------------------------------------------------------------------===//
class Parameter {
 public:
  Parameter(bool is_constant, type::Type::TypeId *typeId,
            type::Value value, int param_idx)
          : is_constant_(is_constant), typeId_(typeId),
            value_(value), param_idx_(param_idx) {}

  // Called at runtime to finalize type in param translator
  void FinalizeType(type::Type::TypeId typeId) {
    *typeId_ = typeId;
  }

  type::Value GetValue() {
    return value_;
  }

  bool IsConstant() {
    return is_constant_;
  }

  int GetParamIdx() {
    return param_idx_;
  }

 private:
  // Distinguish whether the structure comes from constant/param value
  bool is_constant_;

  // Structure that comes from param value holds the ptr
  // Since param is passed during runtime
  // Type has to be determined at runtime
  type::Type::TypeId *typeId_;

  // Dummy value if the structure comes from param value
  type::Value value_;

  // Index to executor context that holds params
  int param_idx_;
};

}  // namespace codegen
}  // namespace peloton