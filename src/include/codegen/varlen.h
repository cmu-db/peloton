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
#include "codegen/runtime_functions_proxy.h"
#include "codegen/varlen_proxy.h"

namespace peloton {
namespace codegen {

class Varlen {
 public:
  // Constructor
  Varlen(llvm::Value *varlen_ptr) : varlen_ptr_(varlen_ptr) {}

  // Get the length and the pointer to the variable length object
  std::pair<llvm::Value *, llvm::Value *> GetObjectAndLength(CodeGen &codegen) {
    auto *varlen_type = VarlenProxy::GetType(codegen);
    PL_ASSERT(varlen_ptr_->getType()->isPointerTy() &&
              varlen_ptr_->getType()->getContainedType(0) == varlen_type);

    auto *len_ptr =
        codegen->CreateConstInBoundsGEP2_32(varlen_type, varlen_ptr_, 0, 0);
    auto *data_ptr =
        codegen->CreateConstInBoundsGEP2_32(varlen_type, varlen_ptr_, 0, 1);

    auto *len = codegen->CreateLoad(codegen.Int32Type(), len_ptr);
    //auto *data = codegen->CreateLoad(codegen.CharPtrType(), data_ptr);

    return std::make_pair(data_ptr, len);
  }

 private:
  llvm::Value *varlen_ptr_;
};

}  // namespace codegen
}  // namespace peloton