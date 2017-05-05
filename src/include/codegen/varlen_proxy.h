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
  // Get the LLVM type of a Peloton varchar data field
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kVarlenTypeName = "peloton::Varlen";
    auto *llvm_type = codegen.LookupTypeByName(kVarlenTypeName);
    if (llvm_type != nullptr) {
      return llvm_type;
    }

    // Not registered in module, construct it now

    // A varlen is just a length-prefixed string, using 4-byte for the length
    auto *varlen_type = llvm::StructType::create(
        codegen.GetContext(), {codegen.Int32Type(), codegen.ByteType()},
        kVarlenTypeName);
    return varlen_type;
  }
};

}  // namespace codegen
}  // namespace peloton