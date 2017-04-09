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

#include "codegen/thread_pool_proxy.h"
#include "codegen/query_thread_pool.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Return the LLVM type that matches the memory layout of our Database class
//===----------------------------------------------------------------------===//
llvm::Type* ThreadPoolProxy::GetType(CodeGen& codegen) {
  static const std::string kThreadPoolTypeName = "peloton::QueryThreadPool";
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
const std::string& ThreadPoolProxy::_SubmitQueryTask::GetFunctionName() {
  static const std::string kThreadPoolProxyFnName =
#ifdef __APPLE__
      // TODO: these symbols are incorrect!!
      "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj11";
#else
      "_ZNK7peloton7catalog7Catalog15GetTableWithOidEjj11";
#endif
  return kThreadPoolProxyFnName;
}

//===----------------------------------------------------------------------===//
// Return the LLVM function definition for QueryThreadPool::SubmitQueryTask()
//===----------------------------------------------------------------------===//
llvm::Function* ThreadPoolProxy::_SubmitQueryTask::GetFunction(CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::Type* thread_pool_type = ThreadPoolProxy::GetType(codegen);

  // create the type for the function to be submitted to thread pool
  std::vector<llvm::Type*> thread_fn_args{codegen.Int64Type(), codegen.Int64Type()};
  llvm::FunctionType* thread_fn_type =
      llvm::FunctionType::get(codegen.Int64Type(), thread_fn_args, false);

  // now create the type for SubmitQueryTask()
  std::vector<llvm::Type*> fn_args{thread_pool_type->getPointerTo(),
                                   thread_fn_type,   // 1st arg: function
                                   codegen.Int64Type(),   // 2nd arg: start
                                   codegen.Int64Type()};  // 3rd arg: end
  llvm::FunctionType* fn_type =
      llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
