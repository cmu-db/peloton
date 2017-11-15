//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen.h
//
// Identification: src/include/codegen/varlen.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/lang/if.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/varlen_proxy.h"

namespace peloton {
namespace codegen {

class Varlen {
 public:
  // Get the length and the pointer to the variable length object
  static void SafeGetPtrAndLength(CodeGen &codegen, llvm::Value *varlen_ptr,
                                  llvm::Value *&data_ptr, llvm::Value *&len) {
    auto *varlen_type = VarlenProxy::GetType(codegen);

    // The first four bytes are the length, load it here
    auto *len_ptr =
        codegen->CreateConstInBoundsGEP2_32(varlen_type, varlen_ptr, 0, 0);
    len = codegen->CreateLoad(codegen.Int32Type(), len_ptr);

    // The four bytes after the start is where the (contiguous) data is
    data_ptr =
        codegen->CreateConstInBoundsGEP2_32(varlen_type, varlen_ptr, 0, 1);
  }

  // Get the length and the pointer to the variable length object
  static void GetPtrAndLength(CodeGen &codegen, llvm::Value *varlen_ptr,
                              llvm::Value *&data_ptr, llvm::Value *&len,
                              llvm::Value *&is_null) {
    // First check if varlen_ptr (i.e., the Varlen *) is NULL
    auto *null_varlen =
        codegen.NullPtr(VarlenProxy::GetType(codegen)->getPointerTo());
    is_null = codegen->CreateICmpEQ(varlen_ptr, null_varlen);

    // Depending on NULL-ness, perform load
    llvm::Value *null_data = nullptr, *null_len = nullptr;
    lang::If varlen_is_null{codegen, is_null};
    {
      // The pointer is null
      null_data = codegen.Null(codegen.CharPtrType());
      null_len = codegen.Const32(0);
    }
    varlen_is_null.ElseBlock();
    {
      // The pointer is not null, safely load the data and length values
      SafeGetPtrAndLength(codegen, varlen_ptr, data_ptr, len);
    }
    varlen_is_null.EndIf();

    // Build PHI nodes for the data pointer and length
    data_ptr = varlen_is_null.BuildPHI(null_data, data_ptr);
    len = varlen_is_null.BuildPHI(null_len, len);
  }
};

}  // namespace codegen
}  // namespace peloton
