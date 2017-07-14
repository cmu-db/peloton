//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_proxy.cpp
//
// Identification: src/codegen/proxy/tile_group_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/tile_group_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(TileGroup, "peloton::storage::TileGroup", MEMBER(opaque));

DEFINE_METHOD(TileGroup, GetNextTupleSlot,
              &peloton::storage::TileGroup::GetNextTupleSlot,
              "_ZNK7peloton7storage9TileGroup16GetNextTupleSlotEv");

DEFINE_METHOD(TileGroup, GetTileGroupId,
              &peloton::storage::TileGroup::GetTileGroupId,
              "_ZNK7peloton7storage9TileGroup14GetTileGroupIdEv");

}  // namespace codegen
}  // namespace peloton