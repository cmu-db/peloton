//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_factory.cpp
//
// Identification: src/backend/storage/tile_group_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/tile_group_factory.h"
#include "backend/storage/tile_group_header.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

// Logging mode
extern LoggingType peloton_logging_mode;

namespace peloton {
namespace storage {

TileGroup *TileGroupFactory::GetTileGroup(
    oid_t database_id, oid_t table_id, oid_t tile_group_id,
    AbstractTable *table, const std::vector<catalog::Schema> &schemas,
    const column_map_type &column_map, int tuple_count) {

  // Allocate the data on appropriate backend
  BackendType backend_type = GetBackendType(peloton_logging_mode);

  TileGroupHeader *tile_header = new TileGroupHeader(backend_type, tuple_count);
  TileGroup *tile_group = new TileGroup(backend_type, tile_header, table,
                                        schemas, column_map, tuple_count);

  tile_group->database_id = database_id;
  tile_group->tile_group_id = tile_group_id;
  tile_group->table_id = table_id;

  return tile_group;
}

}  // End storage namespace
}  // End peloton namespace
