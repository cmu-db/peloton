//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_proxy.h
//
// Identification: src/include/codegen/value_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

class ValueProxy {
 public:
  // Get the LLVM Type for type::Value
  static llvm::Type *GetType(CodeGen &codegen);

  static type::Value *GetValue(type::Value *values, uint32_t offset);

  struct _GetValue {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName();
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton