//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_proxy.h
//
// Identification: src/include/codegen/proxy/tile_group_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "storage/tile_group.h"

namespace peloton {
namespace codegen {

PROXY(TileGroup) {
  PROXY_MEMBER_FIELD(0, char[sizeof(storage::TileGroup)], opaque);
  PROXY_TYPE("peloton::storage::TileGroup", char[sizeof(storage::TileGroup)]);

  PROXY_METHOD(GetNextTupleSlot, &peloton::storage::TileGroup::GetNextTupleSlot,
               "_ZNK7peloton7storage9TileGroup16GetNextTupleSlotEv");
  PROXY_METHOD(GetTileGroupId, &peloton::storage::TileGroup::GetTileGroupId,
               "_ZNK7peloton7storage9TileGroup14GetTileGroupIdEv");
};

namespace proxy {
template <>
struct TypeBuilder<::peloton::storage::TileGroup> {
  using Type = llvm::Type *;
  static Type GetType(CodeGen &codegen) {
    return TileGroupProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton