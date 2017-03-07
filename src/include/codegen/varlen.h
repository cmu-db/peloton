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
  Varlen(llvm::Value *varlen_ptr) : varlen_ptr_(varlen_ptr) {}

  //===--------------------------------------------------------------------===//
  // Get the length and a pointer to the variable length object pointed to
  // by the provided Varlen object.
  // Note that 'varlen_ptr' is a Varlen*
  //===--------------------------------------------------------------------===//
  std::pair<llvm::Value *, llvm::Value *> GetObjectAndLength(CodeGen &codegen) {
    auto *varlen_type = VarlenProxy::GetType(codegen);
    assert(varlen_ptr_->getType()->isPointerTy() &&
           varlen_ptr_->getType()->getContainedType(0) == varlen_type);

    auto *varlen_str_ptr = codegen->CreateLoad(
        codegen->CreateConstInBoundsGEP2_32(varlen_type, varlen_ptr_, 0, 2));
    auto *data_ptr = codegen->CreateConstInBoundsGEP1_32(
        codegen.ByteType(), varlen_str_ptr, sizeof(char *));

    auto *short_len = codegen->CreateZExtOrBitCast(
        codegen->CreateLoad(data_ptr), codegen.Int32Type());
    auto *long_len = codegen->CreateLoad(
        codegen->CreateBitCast(data_ptr, codegen.Int32Type()->getPointerTo()));

    auto *short_val = codegen->CreateConstInBoundsGEP1_32(
        codegen.ByteType(), data_ptr, sizeof(char));
    auto *long_val = codegen->CreateConstInBoundsGEP1_32(
        codegen.ByteType(), data_ptr, 4 * sizeof(char));

    auto *obj_cont_bit = codegen.Const32(1 << 7);
    auto *cont_mask = codegen->CreateAnd(short_len, obj_cont_bit);
    auto *cond = codegen->CreateICmpNE(cont_mask, codegen.Const32(0));

    // Choose which length and val to use based on whether this is a short
    // or long object
    auto *length = codegen->CreateSelect(cond, long_len, short_len);
    auto *val = codegen->CreateSelect(cond, long_val, short_val);

    return std::make_pair(val, length);
  }

 private:
  llvm::Value *varlen_ptr_;
};

}  // namespace codegen
}  // namespace peloton