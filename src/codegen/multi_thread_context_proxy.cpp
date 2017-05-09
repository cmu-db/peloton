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
#include "codegen/barrier.h"
#include "codegen/barrier_proxy.h"
#include "codegen/oa_hash_table_proxy.h"

namespace peloton {
namespace codegen {

llvm::Type *MultiThreadContextProxy::GetType(CodeGen &codegen) {
  static const std::string kMultiThreadContextTypeName = "peloton::codegen::MultiThreadContext";
  // Check if the type is already registered in the module, if so return it
  auto* multithread_context_type = codegen.LookupTypeByName(kMultiThreadContextTypeName);
  if (multithread_context_type != nullptr) {
    return multithread_context_type;
  }

  // Right now we don't need to define each individual field
  // since we only invoke functions on the class.
  static constexpr uint64_t multithread_context_obj_size = sizeof(MultiThreadContext);
  auto* byte_arr_type =
      llvm::ArrayType::get(codegen.Int8Type(), multithread_context_obj_size);
  multithread_context_type = llvm::StructType::create(codegen.GetContext(), {byte_arr_type},
                                          kMultiThreadContextTypeName);
  return multithread_context_type;
}

llvm::Function *MultiThreadContextProxy::InitInstanceFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen18MultiThreadContext12InitInstanceEPS1_llPNS0_7BarrierE";

  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {
          MultiThreadContextProxy::GetType(codegen)->getPointerTo(),
          codegen.Int64Type(),
          codegen.Int64Type(),
          BarrierProxy::GetType(codegen)->getPointerTo()
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
          MultiThreadContextProxy::GetType(codegen)->getPointerTo(),
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
          MultiThreadContextProxy::GetType(codegen)->getPointerTo(),
          codegen.Int64Type()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

llvm::Function *MultiThreadContextProxy::GetThreadIdFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen18MultiThreadContext11GetThreadIdEv";
  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.Int64Type(),
      {MultiThreadContextProxy::GetType(codegen)->getPointerTo()},
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

llvm::Function *MultiThreadContextProxy::GetNotifyMasterFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen18MultiThreadContext12NotifyMasterEv";
  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {
          MultiThreadContextProxy::GetType(codegen)->getPointerTo()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

llvm::Function *MultiThreadContextProxy::GetBarrierWaitFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen18MultiThreadContext11BarrierWaitEv";
  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.BoolType(),
      {
          MultiThreadContextProxy::GetType(codegen)->getPointerTo()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

llvm::Function *MultiThreadContextProxy::GetMergeToGlobalHashTableFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen18MultiThreadContext22MergeToGlobalHashTableEPNS0_5utils11OAHashTableES4_";
  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {
          MultiThreadContextProxy::GetType(codegen)->getPointerTo(),
          OAHashTableProxy::GetType(codegen)->getPointerTo(),
          OAHashTableProxy::GetType(codegen)->getPointerTo()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

} // codegen
} // namespace peloton
