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
  // The proxy around ValuesRuntime::OutputTinyInt()
  struct _OutputTinyInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::OutputSmallInt()
  struct _OutputSmallInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::OutputInteger()
  struct _OutputInteger {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::OutputBigInt()
  struct _OutputBigInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::OutputTimestamp()
  struct _OutputTimestamp {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::OutputDecimal()
  struct _OutputDouble {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::OutputVarchar()
  struct _OutputVarchar {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::OutputVarbinary()
  struct _OutputVarbinary {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuesRuntime::CompareStrings()
  struct _CompareStrings {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
