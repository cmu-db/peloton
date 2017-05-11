#include "codegen/codegen.h"
#include "codegen/barrier.h"
#include "codegen/barrier_proxy.h"

namespace peloton {
namespace codegen {

llvm::Type *BarrierProxy::GetType(CodeGen &codegen) {
  static const std::string kBarrierTypeName = "peloton::codegen::Barrier";
  // Check if the type is already registered in the module, if so return it
  auto* barrier_type = codegen.LookupTypeByName(kBarrierTypeName);
  if (barrier_type != nullptr) {
    return barrier_type;
  }

  static constexpr uint64_t multithread_context_obj_size = sizeof(Barrier);
  auto* byte_arr_type =
      llvm::ArrayType::get(codegen.Int8Type(), multithread_context_obj_size);
  barrier_type = llvm::StructType::create(codegen.GetContext(), {byte_arr_type},
                                          kBarrierTypeName);
  return barrier_type;
}

llvm::Function *BarrierProxy::GetInitInstanceFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen7Barrier12InitInstanceEPS1_m";

  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {
          BarrierProxy::GetType(codegen)->getPointerTo(),
          codegen.Int64Type()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

llvm::Function *BarrierProxy::GetMasterWaitFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen7Barrier10MasterWaitEv";
  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {
          BarrierProxy::GetType(codegen)->getPointerTo()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

llvm::Function *BarrierProxy::GetDestroyFunction(CodeGen &codegen) {
  static const std::string func_name = "_ZN7peloton7codegen7Barrier7DestroyEv";
  auto *func = codegen.LookupFunction(func_name);
  if (func != nullptr) {
    return func;
  }
  // Not cached, create the type
  auto *fn_type = llvm::FunctionType::get(
      codegen.VoidType(),
      {
          BarrierProxy::GetType(codegen)->getPointerTo()
      },
      false);
  return codegen.RegisterFunction(func_name, fn_type);
}

} // codegen
} // namespace peloton
