//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions_proxy.cpp
//
// Identification: src/codegen/runtime_functions_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/codegen/proxy/runtime_functions_proxy.h"

#include "include/codegen/proxy/data_table_proxy.h"
#include "include/codegen/proxy/tile_group_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Get the LLVM function definition/wrapper to our crc64Hash() function
//===----------------------------------------------------------------------===//
llvm::Function *RuntimeFunctionsProxy::_CRC64Hash::GetFunction(
    CodeGen &codegen) {
  static const std::string kHashCrc64FnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen16RuntimeFunctions9HashCrc64EPKcmm";
#else
      "_ZN7peloton7codegen16RuntimeFunctions9HashCrc64EPKcmm";
#endif

  auto *hash_func = codegen.LookupFunction(kHashCrc64FnName);
  if (hash_func != nullptr) {
    return hash_func;
  }

  // Register the RuntimeFunction::HashCrc64() function
  auto &context = codegen.GetContext();
  auto *func_type =
      llvm::TypeBuilder<uint64_t(const char *, uint64_t, uint64_t), false>::get(
          context);
  return codegen.RegisterFunction(kHashCrc64FnName, func_type);
}

//===----------------------------------------------------------------------===//
// Get the LLVM function definition/wrapper to
// RuntimeFunctions::GetTileGroup(DataTable*, oid_t)
//===----------------------------------------------------------------------===//
llvm::Function *RuntimeFunctionsProxy::_GetTileGroup::GetFunction(
    CodeGen &codegen) {
  static const std::string kGetTileGroupFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen16RuntimeFunctions12GetTileGroupEPNS_"
      "7storage9DataTableEj";
#else
      "_ZN7peloton7codegen16RuntimeFunctions12GetTileGroupEPNS_"
      "7storage9DataTableEj";
#endif
  auto *get_tg_func = codegen.LookupFunction(kGetTileGroupFnName);
  if (get_tg_func != nullptr) {
    return get_tg_func;
  }
  // Not cached, create the type
  std::vector<llvm::Type *> fn_args = {
      DataTableProxy::GetType(codegen)->getPointerTo(), codegen.Int64Type()};
  auto *fn_type = llvm::FunctionType::get(
      TileGroupProxy::GetType(codegen)->getPointerTo(), fn_args, false);
  return codegen.RegisterFunction(kGetTileGroupFnName, fn_type);
}

//===----------------------------------------------------------------------===//
//===----------------------------------------------------------------------===//
llvm::Type *RuntimeFunctionsProxy::_ColumnLayoutInfo::GetType(
    CodeGen &codegen) {
  static const std::string kColumnLayoutInfoType = "peloton::ColumnLayoutInfo";
  auto *cli_type = codegen.LookupTypeByName(kColumnLayoutInfoType);
  if (cli_type != nullptr) {
    return cli_type;
  }

  // struct RuntimeFunctions::ColumnLayoutInfo
  std::vector<llvm::Type *> elements = {
      codegen.CharPtrType(), codegen.Int32Type(), codegen.BoolType()};
  cli_type = llvm::StructType::create(codegen.GetContext(), elements,
                                      kColumnLayoutInfoType);
  return cli_type;
}

//===----------------------------------------------------------------------===//
// Get the LLVM function definition/wrapper to
// RuntimeFunctions::GetTileGroupLayout()
//===----------------------------------------------------------------------===//
llvm::Function *RuntimeFunctionsProxy::_GetTileGroupLayout::GetFunction(
    CodeGen &codegen) {
  static const std::string kGetTileGroupLayoutFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen16RuntimeFunctions18GetTileGroupLayoutEPKNS_"
      "7storage9TileGroupEPNS1_16ColumnLayoutInfoEj";
#else
      "_ZN7peloton7codegen16RuntimeFunctions18GetTileGroupLayoutEPKNS_"
      "7storage9TileGroupEPNS1_16ColumnLayoutInfoEj";
#endif

  auto *get_tg_func = codegen.LookupFunction(kGetTileGroupLayoutFnName);
  if (get_tg_func != nullptr) {
    return get_tg_func;
  }
  // Function arguments: DataTable*, oid_t, ColumnLayoutInfo*, uint32_t
  std::vector<llvm::Type *> fn_args = {
      TileGroupProxy::GetType(codegen)->getPointerTo(),
      RuntimeFunctionsProxy::_ColumnLayoutInfo::GetType(codegen)
          ->getPointerTo(),
      codegen.Int32Type()};
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
  return codegen.RegisterFunction(kGetTileGroupLayoutFnName, fn_type);
}

//===----------------------------------------------------------------------===//
// Get the LLVM function definition/wrapper to
// RuntimeFunctions::ThrowDivideByZeroException()
//===----------------------------------------------------------------------===//
llvm::Function *RuntimeFunctionsProxy::_ThrowDivideByZeroException::GetFunction(
    CodeGen &codegen) {
  static const std::string kDivZeroExceptionFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen16RuntimeFunctions26ThrowDivideByZeroExceptionEv";
#else
      "_ZN7peloton7codegen16RuntimeFunctions26ThrowDivideByZeroExceptionEv";
#endif

  auto *get_exc_func = codegen.LookupFunction(kDivZeroExceptionFnName);
  if (get_exc_func != nullptr) {
    return get_exc_func;
  }
  // No function args or return values
  std::vector<llvm::Type *> fn_args;
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), false);
  return codegen.RegisterFunction(kDivZeroExceptionFnName, fn_type);
}

//===----------------------------------------------------------------------===//
// Get the LLVM function definition/wrapper to
// RuntimeFunctions::ThrowOverflowException()
//===----------------------------------------------------------------------===//
llvm::Function *RuntimeFunctionsProxy::_ThrowOverflowException::GetFunction(
    CodeGen &codegen) {
  static const std::string kOverflowExceptionFnName =
#ifdef __APPLE__
      "_ZN7peloton7codegen16RuntimeFunctions22ThrowOverflowExceptionEv";
#else
      "_ZN7peloton7codegen16RuntimeFunctions22ThrowOverflowExceptionEv";
#endif

  auto *get_exc_func = codegen.LookupFunction(kOverflowExceptionFnName);
  if (get_exc_func != nullptr) {
    return get_exc_func;
  }
  // No function args or return values
  std::vector<llvm::Type *> fn_args;
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), false);
  return codegen.RegisterFunction(kOverflowExceptionFnName, fn_type);
}

}  // namespace codegen
}  // namespace peloton
