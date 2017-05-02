//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_thread_pool_proxy.cpp
//
// Identification: src/codegen/query_thread_pool_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/multi_thread_context_proxy.h"
#include "codegen/query_thread_pool_proxy.h"
#include "codegen/query_thread_pool.h"
#include "codegen/runtime_state.h"

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

llvm::Function *QueryThreadPoolProxy::GetGetIntanceFunction(CodeGen &codegen) {
    const std::string& fn_name = "_ZN7peloton7codegen15QueryThreadPool11GetInstanceEv";

    // Has the function already been registered?
    llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
    if (llvm_fn != nullptr) {
        return llvm_fn;
    }

    // The function hasn't been registered, let's do it now
    llvm::Type* thread_pool_type = QueryThreadPoolProxy::GetType(codegen);

    llvm::FunctionType* fn_type = llvm::FunctionType::get(thread_pool_type->getPointerTo(), {}, false);
    return codegen.RegisterFunction(fn_name, fn_type);
}

llvm::Function *QueryThreadPoolProxy::GetSubmitQueryTaskFunction(CodeGen &codegen, RuntimeState *runtime_state) {
  const std::string& fn_name = "_ZN7peloton7codegen15QueryThreadPool15SubmitQueryTaskEPNS0_12RuntimeStateEPNS0_18MultiThreadContextEPFvS3_S5_E";

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::Type* thread_pool_type = QueryThreadPoolProxy::GetType(codegen);

  auto target_func_type = llvm::FunctionType::get(codegen.VoidType(), {
      runtime_state->FinalizeType(codegen)->getPointerTo(),
      MultiThreadContextProxy::GetType(codegen)->getPointerTo()
  }, false);

  llvm::FunctionType* fn_type = llvm::FunctionType::get(
    codegen.VoidType(), {
      thread_pool_type->getPointerTo(),
      runtime_state->FinalizeType(codegen)->getPointerTo(),
      MultiThreadContextProxy::GetType(codegen)->getPointerTo(),
      target_func_type->getPointerTo()
    }, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

llvm::Function *QueryThreadPoolProxy::GetThreadCountFunction(CodeGen &codegen) {
  const std::string& fn_name = "_ZN7peloton7codegen15QueryThreadPool14GetThreadCountEv";

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
      return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  llvm::FunctionType* fn_type = llvm::FunctionType::get(codegen.Int64Type(), {}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
