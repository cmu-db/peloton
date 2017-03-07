//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// values_runtime_proxy.h
//
// Identification: src/include/codegen/values_runtime_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class ValuesRuntimeProxy {
 public:
  // Get the LLVM Type for common::Value
  static llvm::Type *GetType(CodeGen &codegen);

  // The proxy around ValuesRuntime::outputTinyInt()
  struct _outputTinyInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputSmallInt()
  struct _outputSmallInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputInteger()
  struct _outputInteger {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputBigInt()
  struct _outputBigInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputDouble()
  struct _outputDouble {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputVarchar()
  struct _outputVarchar {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
