//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_proxy.h
//
// Identification: src/include/codegen/varlen_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/varlen_type.h"

namespace peloton {
namespace codegen {

class VarlenProxy {
 public:
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kVarlenTypeName = "peloton::Varlen";
    auto *llvm_type = codegen.LookupTypeByName(kVarlenTypeName);
    if (llvm_type != nullptr) {
      return llvm_type;
    }

    // Sanity check
//    static_assert(
//        sizeof(type::VarlenType) == 24,
//        "The LLVM memory layout of Varlen doesn't match the pre-compiled "
//        "version. Did you forget to update codegen/varlen_proxy.cpp?");

    // Not registered in module, construct it now
    std::vector<llvm::Type *> parts = {codegen.Int64Type(), codegen.Int64Type(),
                                       codegen.CharPtrType()};
    auto *varlen_type =
        llvm::StructType::create(codegen.GetContext(), parts, kVarlenTypeName);
    return varlen_type;
  }
};

}  // namespace codegen
}  // namespace peloton