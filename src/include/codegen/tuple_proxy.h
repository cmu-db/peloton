//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_proxy.h
//
// Identification: src/include/codegen/tuple_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "storage/tuple.h"

namespace peloton {
namespace codegen {

class TupleProxy {
 public:
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kTupleTypeName = "peloton::storage:Tuple";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto tuple_type = codegen.LookupTypeByName(kTupleTypeName);
    if (tuple_type != nullptr) {
      return tuple_type;
    }

    // Type isn't cached, create a new one
    auto *opaque_byte_array = llvm::ArrayType::get(codegen.Int8Type(),
                                                   sizeof(storage::Tuple));
    return llvm::StructType::create(codegen.GetContext(), {opaque_byte_array},
                                    kTupleTypeName);
  }
};

}  // namespace codegen
}  // namespace peloton
