//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_runtime_proxy.h
//
// Identification: src/include/codegen/update/update_runtime_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/types.h"
#include "planner/project_info.h"

namespace peloton {
namespace codegen {

class UpdateRuntimeProxy {
 public:
  // Get the LLVM type for Target
  static llvm::Type *GetTargetType(CodeGen &codegen) {
    static const std::string kTargetName = "peloton::Target";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto target_type = codegen.LookupTypeByName(kTargetName);
    if (target_type != nullptr) {
      return target_type;
    }

    // Type isn't cached, create a new one
    auto *opaque_byte_array = llvm::ArrayType::get(
        codegen.Int8Type(), sizeof(Target));
    return llvm::StructType::create(codegen.GetContext(), {opaque_byte_array},
                                    kTargetName);
  }

  // Get the LLVM type for DirectMap
  static llvm::Type *GetDirectMapType(CodeGen &codegen) {
    static const std::string kDirectMapName = "peloton::DirectMap";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto direct_map_type = codegen.LookupTypeByName(kDirectMapName);
    if (direct_map_type != nullptr) {
        return direct_map_type;
    }

    // Type isn't cached, create a new one
    auto *opaque_byte_array = llvm::ArrayType::get(
            codegen.Int8Type(), sizeof(DirectMap));
    return llvm::StructType::create(codegen.GetContext(), {opaque_byte_array},
                                    kDirectMapName);
  }

  // Get the LLVM type for ExecutorContext
  static llvm::Type *GetExecContextType(CodeGen &codegen) {
    static const std::string kExecContextName = "peloton::executor::ExecutorContext";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto exec_context_type = codegen.LookupTypeByName(kExecContextName);
    if (exec_context_type != nullptr) {
        return exec_context_type;
    }

    // Type isn't cached, create a new one
    auto *opaque_byte_array = llvm::ArrayType::get(
            codegen.Int8Type(), sizeof(DirectMap));
    return llvm::StructType::create(codegen.GetContext(), {opaque_byte_array},
                                    kExecContextName);
  }
};

}  // namespace codegen
}  // namespace peloton