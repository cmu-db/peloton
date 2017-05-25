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
#include "executor/executor_context.h"

namespace peloton {
namespace codegen {

class ExecutorContextProxy {
 public:
  // Get the LLVM type for ExecutorContext
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kExecutorContextName =
        "peloton::executor::ExecutorContext";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto executor_context_type = codegen.LookupTypeByName(kExecutorContextName);
    if (executor_context_type != nullptr) {
      return executor_context_type;
    }

    // Type isn't cached, create a new one
    auto *opaque_byte_array = llvm::ArrayType::get(codegen.Int8Type(),
        sizeof(executor::ExecutorContext));
    return llvm::StructType::create(codegen.GetContext(), {opaque_byte_array},
                                    kExecutorContextName);
  }
};

}  // namespace codegen
}  // namespace peloton
