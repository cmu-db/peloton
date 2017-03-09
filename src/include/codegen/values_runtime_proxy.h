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
  // The proxy around ValuesRuntime::outputTinyInt()
  struct _OutputTinyInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputSmallInt()
  struct _OutputSmallInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputInteger()
  struct _OutputInteger {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputBigInt()
  struct _OutputBigInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputDouble()
  struct _OutputDouble {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::outputVarchar()
  struct _OutputVarchar {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
