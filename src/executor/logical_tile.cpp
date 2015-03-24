/*-------------------------------------------------------------------------
 *
 * logical_tile.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/logical_tile.cpp
 *
 *-------------------------------------------------------------------------
 */


#include "executor/logical_tile.h"
#include "storage/tile.h"

#include <iostream>
#include <cassert>

namespace nstore {
namespace executor {

// Add a tuple set to the container
void LogicalTile::AppendTupleSet(std::vector<catalog::ItemPointer> tuple_set) {
  assert(tuple_set.size() == base_tile_count);

  tuple_set_container.push_back(tuple_set);
}

// Add a tuple to the container at the given offset
void LogicalTile::AppendTuple(id_t offset, catalog::ItemPointer tuple) {
  assert(offset < tuple_set_container.size());

  tuple_set_container[offset].push_back(tuple);

}

// Get the tuple from given tile at the given tuple offset
storage::Tuple *LogicalTile::GetTuple(id_t tile_offset, id_t tuple_offset) {
  assert(tuple_offset < tuple_set_container.size());

  catalog::ItemPointer item_pointer = tuple_set_container[tuple_offset][tile_offset];

  storage::Tile *tile = reinterpret_cast<storage::Tile *>(catalog->locator[item_pointer.tile_id]);

  storage::Tuple *tuple = tile->GetTuple(tuple_offset);

  return tuple;
}


std::ostream& operator<<(std::ostream& os, const LogicalTile& logical_tile) {

  os << "\t-----------------------------------------------------------\n";

  os << "\tLOGICAL TILE\n";

  for(auto tuple_set : logical_tile.tuple_set_container){
    for(auto tuple : tuple_set) {
      os << "\t" << tuple.tile_id << " " << tuple.offset << "\n";
    }
  }

  os << "\t-----------------------------------------------------------\n";

  return os;
}


} // End executor namespace
} // End nstore namespace
