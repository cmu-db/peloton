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

  id_t tile_group_id = catalog->GetNextOid();
  TileGroupHeader *tile_group_header = new TileGroupHeader(backend, tuple_count);

  std::vector<catalog::Schema*> schemas;
  std::vector<std::vector<std::string> > column_names;

  schemas.push_back(schema);

  TileGroup* tile_group = TileGroupFactory::GetTileGroup(database_id, table_id, tile_group_id,
                                                         tile_group_header, schemas, catalog,
                                                         backend, tuple_count,
                                                         column_names, own_schema);

  {
    std::lock_guard<std::mutex> lock(table_mutex);
    tile_groups.push_back(tile_group);
  }

  return tile_group_id;
}


} // End storage namespace
} // End nstore namespace

