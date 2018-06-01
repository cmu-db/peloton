//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_proxy.cpp
//
// Identification: src/codegen/proxy/tile_group_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/tile_group_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(TileGroup, "peloton::storage::TileGroup", opaque);

DEFINE_METHOD(peloton::storage, TileGroup, GetNextTupleSlot);
DEFINE_METHOD(peloton::storage, TileGroup, GetTileGroupId);

}  // namespace codegen
}  // namespace peloton
