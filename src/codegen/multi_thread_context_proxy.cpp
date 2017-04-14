//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// multi_thread_context_proxy.cpp
//
// Identification: src/codegen/multi_thread_context_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen.h"
#include "codegen/multi_thread_context.h"
#include "codegen/multi_thread_context_proxy.h"

namespace peloton {
namespace codegen {

llvm::Type *MultiThreadContextProxy::GetType(CodeGen &codegen) {
  static const std::string kThreadPoolTypeName = "peloton::codegen::MultiThreadContext";
  // Check if the type is already registered in the module, if so return it
  auto* thread_pool_type = codegen.LookupTypeByName(kThreadPoolTypeName);
  if (thread_pool_type != nullptr) {
    return thread_pool_type;
  }

  // Right now we don't need to define each individual field
  // since we only invoke functions on the class.
  static constexpr uint64_t thread_pool_obj_size = sizeof(MultiThreadContext);
  auto* byte_arr_type =
      llvm::ArrayType::get(codegen.Int8Type(), thread_pool_obj_size);
  thread_pool_type = llvm::StructType::create(codegen.GetContext(), {byte_arr_type},
                                          kThreadPoolTypeName);
  return thread_pool_type;
}

llvm::Function *MultiThreadContextProxy::GetInstanceFunction(UNUSED_ATTRIBUTE CodeGen &codegen) {
  static std::string func_name = "_ZN7peloton7codegen18MultiThreadContextC1Emm";
  // TODO(tq5124): fix this.
  return nullptr;
}

llvm::Function *MultiThreadContextProxy::GetRangeStartFunction(UNUSED_ATTRIBUTE CodeGen &codegen) {
  static std::string func_name = "_ZN7peloton7codegen18MultiThreadContext13GetRangeStartEmm";
  // TODO(tq5124): fix this.
  return nullptr;
}

llvm::Function *MultiThreadContextProxy::GetRangeEndFunction(UNUSED_ATTRIBUTE CodeGen &codegen) {
  static std::string func_name = "_ZN7peloton7codegen18MultiThreadContext11GetRangeEndEmm";
  // TODO(tq5124): fix this.
  return nullptr;
}

} // codegen
} // namespace peloton
