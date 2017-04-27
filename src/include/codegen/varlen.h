//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen.h
//
// Identification: src/include/codegen/varlen.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/if.h"
#include "codegen/runtime_functions_proxy.h"
#include "codegen/varlen_proxy.h"

namespace peloton {
namespace codegen {

class Varlen {
 public:
  /**
   * @brief Get the length and the pointer to the variable length object.
   *
   * SafeGetPtrAndLength(peloton::Varlen **varlen_ptr_ptr) {
   *   peloton::Varlen *varlen_ptr = *varlen_ptr_ptr;
   *
   *   int32_t *len_ptr = &varlen_ptr->len;
   *   int32_t len = *len_ptr;
   *   char *data_ptr = &varlen->data;
   *
   *   return (len, data_ptr);
   * }
   */
  static void SafeGetPtrAndLength(CodeGen &codegen, llvm::Value *varlen_ptr_ptr,
                                  llvm::Value *&data_ptr, llvm::Value *&len) {
    // Load the Varlen** to get a Varlen*
    auto *varlen_type = VarlenProxy::GetType(codegen);
    auto *varlen_ptr = codegen->CreateLoad(codegen->CreateBitCast(
        varlen_ptr_ptr, varlen_type->getPointerTo()->getPointerTo()));

    // The first four bytes are the length, load it here
    auto *len_ptr =
        codegen->CreateConstInBoundsGEP2_32(varlen_type, varlen_ptr, 0, 0);
    len = codegen->CreateLoad(codegen.Int32Type(), len_ptr);

    // The four bytes after the start is where the (contiguous) data is
    data_ptr =
        codegen->CreateConstInBoundsGEP2_32(varlen_type, varlen_ptr, 0, 1);
  }

  /**
   * @brief Get the length and the pointer to the variable length object.
   *
   * GetPtrAndLength(peloton::Varlen **varlen_ptr_ptr) {
   *   char *null_ptr = NULL;
   *   bool is_null = varlen_ptr_ptr == null_pr;
   *
   *   if (is_null) {
   *     return (0, NULL);
   *   } else {
   *     return SafeGetPtrAndLength(varlen_ptr_ptr);
   *   }
   * }
   */
  static void GetPtrAndLength(CodeGen &codegen, llvm::Value *varlen_ptr_ptr,
                              llvm::Value *&data_ptr, llvm::Value *&len,
                              llvm::Value *&is_null) {
    // Before loading the components of the Varlen, we need to do a null check
    // on the Varlen** function argument.

    auto *null_ptr = codegen.NullPtr(codegen.CharPtrType());
    is_null = codegen->CreateICmpEQ(varlen_ptr_ptr, null_ptr);

    llvm::Value *null_data = nullptr, *null_len = nullptr;
    If varlen_is_null{codegen, is_null};
    {
      // The pointer is null
      null_data = codegen.Null(codegen.CharPtrType());
      null_len = codegen.Const32(0);
    }
    varlen_is_null.ElseBlock();
    {
      // The pointer is not null, safely load the data and length values
      SafeGetPtrAndLength(codegen, varlen_ptr_ptr, data_ptr, len);
    }
    varlen_is_null.EndIf();

    // Build PHI nodes for the data pointer and length
    data_ptr = varlen_is_null.BuildPHI(null_data, data_ptr);
    len = varlen_is_null.BuildPHI(null_len, len);
  }
};

}  // namespace codegen
}  // namespace peloton
