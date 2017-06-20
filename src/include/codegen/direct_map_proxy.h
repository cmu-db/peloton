//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// direct_map_proxy.h
//
// Identification: src/include/codegen/direct_map_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/types.h"

namespace peloton {
namespace codegen {

class DirectMapProxy {
 public:
  // Get the LLVM type for DirectMap
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kDirectMapName = "peloton::DirectMap";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto direct_map_type = codegen.LookupTypeByName(kDirectMapName);
    if (direct_map_type != nullptr) {
      return direct_map_type;
    }

    // Type isn't cached, create a new one
    auto *opaque_byte_array =
        llvm::ArrayType::get(codegen.Int8Type(), sizeof(DirectMap));
    return llvm::StructType::create(codegen.GetContext(), {opaque_byte_array},
                                    kDirectMapName);
  }
};

}  // namespace codegen
}  // namespace peloton
