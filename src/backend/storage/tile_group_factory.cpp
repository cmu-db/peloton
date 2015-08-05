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

namespace peloton {
namespace storage {

TileGroup *TileGroupFactory::GetTileGroup(
    oid_t database_id, oid_t table_id, oid_t tile_group_id,
    AbstractTable *table, AbstractBackend *backend,
    const std::vector<catalog::Schema> &schemas,
    const column_map_type &column_map, int tuple_count) {
  TileGroupHeader *tile_header = new TileGroupHeader(backend, tuple_count);
  TileGroup *tile_group = new TileGroup(tile_header, table, backend, schemas,
                                        column_map, tuple_count);

  // TileGroupFactory::InitCommon(tile_group, database_id, table_id,
  // tile_group_id);
  tile_group->database_id = database_id;
  tile_group->tile_group_id = tile_group_id;
  tile_group->table_id = table_id;

  return tile_group;
}

}  // End storage namespace
}  // End peloton namespace
