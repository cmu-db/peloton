//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_proxy.h
//
// Identification: src/include/codegen/proxy/tile_group_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "storage/tile_group.h"

namespace peloton {
namespace codegen {

PROXY(TileGroup) {
  DECLARE_MEMBER(0, char[sizeof(storage::TileGroup)], opaque);
  DECLARE_TYPE;

  DECLARE_METHOD(GetNextTupleSlot);
  DECLARE_METHOD(GetTileGroupId);
};

TYPE_BUILDER(TileGroup, storage::TileGroup);

}  // namespace codegen
}  // namespace peloton