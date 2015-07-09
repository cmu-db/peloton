/*-------------------------------------------------------------------------
 *
 * database.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/table.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/storage/database.h"

namespace peloton {
namespace storage {

std::ostream& operator<<(std::ostream& os, const Database& database) {
/*
  os << "=====================================================\n";
  os << "DATABASE :\n";

  oid_t tile_group_count = table.GetTileGroupCount();
  std::cout << "Tile Group Count : " << tile_group_count << "\n";

  oid_t tuple_count = 0;
  for (oid_t tile_group_itr = 0 ; tile_group_itr < tile_group_count ; tile_group_itr++) {
    auto tile_group = table.GetTileGroup(tile_group_itr);
    auto tile_tuple_count = tile_group->GetNextTupleSlot();

    std::cout << "Tile Group Id  : " << tile_group_itr << " Tuple Count : " << tile_tuple_count << "\n";
    std::cout << (*tile_group);

    tuple_count += tile_tuple_count;
  }

  std::cout << "Table Tuple Count :: " << tuple_count << "\n";

  os << "=====================================================\n";

*/
  return os;
}

} // End storage namespace
} // End peloton namespace

