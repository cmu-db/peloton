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

llvm::Function *MultiThreadContextProxy::GetInstanceFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen18MultiThreadContext11GetInstanceEll";

  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      MultiThreadContextProxy::GetType(codegen),
      {
          codegen.Int64Type(),
          codegen.Int64Type()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

llvm::Function *MultiThreadContextProxy::GetRangeStartFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen18MultiThreadContext13GetRangeStartEl";
  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.Int64Type(),
      {
          MultiThreadContextProxy::GetType(codegen),
          codegen.Int64Type()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

llvm::Function *MultiThreadContextProxy::GetRangeEndFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen18MultiThreadContext11GetRangeEndEl";
  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.Int64Type(),
      {
          MultiThreadContextProxy::GetType(codegen),
          codegen.Int64Type()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

} // codegen
} // namespace peloton
