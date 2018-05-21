//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_factory.cpp
//
// Identification: src/storage/tile_group_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/tile_group_factory.h"
// #include "logging/logging_util.h"
#include "storage/tile_group_header.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

// Logging mode
extern peloton::LoggingType peloton_logging_mode;

namespace peloton {
namespace storage {

TileGroup *TileGroupFactory::GetTileGroup(
    oid_t database_id, oid_t table_id, oid_t tile_group_id,
    AbstractTable *table, const std::vector<catalog::Schema> &schemas,
    std::shared_ptr<const Layout> layout, int tuple_count) {
  // Allocate the data on appropriate backend
  BackendType backend_type = BackendType::MM;
      // logging::LoggingUtil::GetBackendType(peloton_logging_mode);
  // Ensure that the layout of the new TileGroup is not null.
  if (layout == nullptr) {
    throw NullPointerException("Layout of the TileGroup must be non-null.");
  }

  TileGroupHeader *tile_header = new TileGroupHeader(backend_type, tuple_count);
  TileGroup *tile_group = new TileGroup(backend_type, tile_header, table,
                                        schemas, layout, tuple_count);

  tile_header->SetTileGroup(tile_group);

  tile_group->database_id = database_id;
  tile_group->tile_group_id = tile_group_id;
  tile_group->table_id = table_id;

  return tile_group;
}

}  // namespace storage
}  // namespace peloton
