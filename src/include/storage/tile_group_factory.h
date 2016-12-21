//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_factory.h
//
// Identification: src/include/storage/tile_group_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/manager.h"
#include "storage/abstract_table.h"
#include "storage/tile_group.h"

#include <string>

namespace peloton {
namespace storage {

/**
 * Super Awesome TileGroupFactory!!
 */
class TileGroupFactory {
 public:
  TileGroupFactory();
  virtual ~TileGroupFactory();

  static TileGroup *GetTileGroup(oid_t database_id, oid_t table_id,
                                 oid_t tile_group_id, AbstractTable *table,
                                 const std::vector<catalog::Schema> &schemas,
                                 const column_map_type &column_map,
                                 int tuple_count);
};

}  // End storage namespace
}  // End peloton namespace
