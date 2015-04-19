/*-------------------------------------------------------------------------
 *
 * table.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/table.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/table.h"

namespace nstore {
namespace storage {

id_t Table::AddTileGroup() {

  std::vector<catalog::Schema> schemas;
  std::vector<std::vector<std::string> > column_names;

  id_t tile_group_id = catalog->GetNextOid();
  schemas.push_back(schema);

  TileGroup* tile_group = TileGroupFactory::GetTileGroup(database_id, table_id, tile_group_id,
                                                         catalog, backend,
                                                         schemas, tuple_count);

  {
    std::lock_guard<std::mutex> lock(table_mutex);
    tile_groups.push_back(tile_group);
  }

  return tile_group_id;
}


} // End storage namespace
} // End nstore namespace

