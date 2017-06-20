//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// target_proxy.h
//
// Identification: src/include/codegen/target_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "planner/project_info.h"
#include "type/types.h"

namespace peloton {
namespace codegen {

class TargetProxy {
 public:
  // Get the LLVM type for Target
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kTargetName = "peloton::Target";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto target_type = codegen.LookupTypeByName(kTargetName);
    if (target_type != nullptr) {
      return target_type;
    }

    // Type isn't cached, create a new one
    auto *opaque_byte_array =
        llvm::ArrayType::get(codegen.Int8Type(), sizeof(peloton::Target));
    return llvm::StructType::create(codegen.GetContext(), {opaque_byte_array},
                                    kTargetName);
  }
};

}  // namespace codegen
}  // namespace peloton
