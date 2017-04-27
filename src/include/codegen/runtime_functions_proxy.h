//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions_proxy.h
//
// Identification: src/include/codegen/runtime_functions_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// The class that proxies various runtime utility functions that can be called
// from a compiled LLVM plan.
//===----------------------------------------------------------------------===//
class RuntimeFunctionsProxy {
 public:
  struct _CRC64Hash {
    // Get the LLVM function definition/wrapper to our crc64Hash() function
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _GetTileGroup {
    // Get the LLVM function definition/wrapper to
    // RuntimeFunctions::GetTileGroup(DataTable*, oid_t)
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _ColumnLayoutInfo {
    static llvm::Type *GetType(CodeGen &codegen);
  };

  struct _GetTileGroupLayout {
    // Get the LLVM function definition/wrapper to
    // RuntimeFunctions::GetTileGroupLayout()
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _ThrowDivideByZeroException {
    // Get the LLVM function definition/wrapper to our
    // ThrowDivideByZeroException() function
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _ThrowOverflowException {
    // Get the LLVM function definition/wrapper to our
    // ThrowOverflowException() function
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
