//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pool_proxy.h
//
// Identification: src/include/codegen/pool_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace codegen {

class PoolProxy {
 public:
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kPoolTypeName = "peloton::type:AbstractPool";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto pool_type = codegen.LookupTypeByName(kPoolTypeName);
    if (pool_type != nullptr) {
      return pool_type;
    }

    // Type isn't cached, create a new one
    auto *opaque_byte_array = llvm::ArrayType::get(codegen.Int8Type(),
                                                   sizeof(type::AbstractPool));
    return llvm::StructType::create(codegen.GetContext(), {opaque_byte_array},
                                    kPoolTypeName);
  }
};

}  // namespace codegen
}  // namespace peloton
