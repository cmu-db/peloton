//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_proxy.h
//
// Identification: src/include/codegen/value_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

class ValueProxy {
 public:
  // Get the LLVM Type for type::Value
  static llvm::Type* GetType(CodeGen& codegen) {
    static const std::string kValueTypeName = "peloton::Value";
    auto* value_type = codegen.LookupTypeByName(kValueTypeName);
    if (value_type != nullptr) {
      return value_type;
    }

    // Type isnt cached, create it
    auto* opaque_arr_type =
        codegen.VectorType(codegen.Int8Type(), sizeof(type::Value));
    return llvm::StructType::create(codegen.GetContext(), {opaque_arr_type},
                                    kValueTypeName);
  }
};

}  // namespace codegen
}  // namespace peloton