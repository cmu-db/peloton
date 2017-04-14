//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_pool_proxy.cpp
//
// Identification: src/codegen/thread_pool_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/multi_thread_context_proxy.h"
#include "codegen/query_thread_pool_proxy.h"
#include "codegen/query_thread_pool.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Return the LLVM type that matches the memory layout of our Database class
//===----------------------------------------------------------------------===//
llvm::Type* QueryThreadPoolProxy::GetType(CodeGen& codegen) {
  static const std::string kThreadPoolTypeName = "peloton::codegen::QueryThreadPool";
  // Check if the type is already registered in the module, if so return it
  auto* thread_pool_type = codegen.LookupTypeByName(kThreadPoolTypeName);
  if (thread_pool_type != nullptr) {
    return thread_pool_type;
  }

  // Right now we don't need to define each individual field
  // since we only invoke functions on the class.
  static constexpr uint64_t thread_pool_obj_size = sizeof(QueryThreadPool);
  auto* byte_arr_type =
      llvm::ArrayType::get(codegen.Int8Type(), thread_pool_obj_size);
  thread_pool_type = llvm::StructType::create(codegen.GetContext(), {byte_arr_type},
                                          kThreadPoolTypeName);
  return thread_pool_type;
}

//===----------------------------------------------------------------------===//
// Return the symbol of the QueryThreadPool::SubmitQueryTask() function
//===----------------------------------------------------------------------===//
const std::string& QueryThreadPoolProxy::_SubmitQueryTask::GetFunctionName() {
  static const std::string kThreadPoolProxyFnName =
      // TODO: these symbols are incorrect!!
      "_ZNK7peloton7codegen15QueryThreadPool15SubmitQueryTask";
  return kThreadPoolProxyFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for QueryThreadPool::SubmitQueryTask()
//===----------------------------------------------------------------------===//
llvm::Function* QueryThreadPoolProxy::_SubmitQueryTask::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::Type* thread_pool_type = QueryThreadPoolProxy::GetType(codegen);

  llvm::FunctionType* fn_type = llvm::FunctionType::get(
    codegen.VoidType(), {
      thread_pool_type->getPointerTo(),
      MultiThreadContextProxy::GetType(codegen)
    }, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
