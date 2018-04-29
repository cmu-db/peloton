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
#include "settings/settings_manager.h"
#include "statistics/backend_stats_context.h"
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
    const column_map_type &column_map, int tuple_count) {
  // Allocate the data on appropriate backend
  BackendType backend_type = BackendType::MM;
  // logging::LoggingUtil::GetBackendType(peloton_logging_mode);

  TileGroupHeader *tile_header = new TileGroupHeader(backend_type, tuple_count);

  // Record memory allocation for tile group header
  if (table_id != INVALID_OID &&
      static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncreaseTableMemoryAlloc(
        database_id, table_id, tile_header->GetHeaderSize());
  }

  TileGroup *tile_group = new TileGroup(backend_type, tile_header, table,
                                        schemas, column_map, tuple_count,
                                        database_id, table_id, tile_group_id);

  tile_header->SetTileGroup(tile_group);

  return tile_group;
}

}  // namespace storage
}  // namespace peloton
