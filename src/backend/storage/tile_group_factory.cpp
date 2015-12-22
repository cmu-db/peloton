//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_group_factory.cpp
//
// Identification: src/backend/storage/tile_group_factory.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/tile_group_factory.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace storage {

TileGroup *TileGroupFactory::GetTileGroup(
    oid_t database_id, oid_t table_id, oid_t tile_group_id,
    AbstractTable *table,
    const std::vector<catalog::Schema> &schemas,
    const column_map_type &column_map, int tuple_count) {

  // Allocate based on backend
  TileGroupHeader *tile_header = new TileGroupHeader(BACKEND_TYPE_MM, tuple_count);
  TileGroup *tile_group = new TileGroup(BACKEND_TYPE_MM,
                                        tile_header, table, schemas,
                                        column_map, tuple_count);

  tile_group->database_id = database_id;
  tile_group->tile_group_id = tile_group_id;
  tile_group->table_id = table_id;

  return tile_group;
}

}  // End storage namespace
}  // End peloton namespace
