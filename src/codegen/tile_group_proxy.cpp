//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_proxy.cpp
//
// Identification: src/codegen/tile_group_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/tile_group_proxy.h"

#include "storage/tile_group.h"

namespace peloton {
namespace codegen {

llvm::Type* TileGroupProxy::GetType(CodeGen& codegen) {
  static const std::string kTileGroupName = "peloton::storage::TileGroup";
  // Check if the data table type has already been registered in the current
  // codegen context
  auto tile_group_type = codegen.LookupTypeByName(kTileGroupName);
  if (tile_group_type != nullptr) {
    return tile_group_type;
  }

  // Type isn't cached, create a new one
  auto* byte_array =
      llvm::ArrayType::get(codegen.Int8Type(), sizeof(storage::TileGroup));
  tile_group_type = llvm::StructType::create(codegen.GetContext(), {byte_array},
                                             kTileGroupName);
  return tile_group_type;
}

const std::string& TileGroupProxy::_GetNextTupleSlot::GetFunctionName() {
  static const std::string kGetNextTupleSlot =
#ifdef __APPLE__
      "_ZNK7peloton7storage9TileGroup16GetNextTupleSlotEv";
#else
      "_ZNK7peloton7storage9TileGroup16GetNextTupleSlotEv";
#endif
  return kGetNextTupleSlot;
}

llvm::Function* TileGroupProxy::_GetNextTupleSlot::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  auto* tile_group_type = TileGroupProxy::GetType(codegen);
  llvm::FunctionType* fn_type = llvm::FunctionType::get(
      codegen.Int32Type(), {tile_group_type->getPointerTo()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

const std::string& TileGroupProxy::_GetTileGroupId::GetFunctionName() {
  static const std::string kGetNextTupleSlot =
#ifdef __APPLE__
      "_ZNK7peloton7storage9TileGroup14GetTileGroupIdEv";
#else
      "_ZNK7peloton7storage9TileGroup14GetTileGroupIdEv";
#endif
  return kGetNextTupleSlot;
}

llvm::Function* TileGroupProxy::_GetTileGroupId::GetFunction(
    CodeGen& codegen) {
  const std::string& fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function* llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  // The function hasn't been registered, let's do it now
  auto* tile_group_type = TileGroupProxy::GetType(codegen);
  llvm::FunctionType* fn_type = llvm::FunctionType::get(
      codegen.Int32Type(), {tile_group_type->getPointerTo()}, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton