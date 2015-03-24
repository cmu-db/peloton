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
storage::Tuple *LogicalTile::GetTuple(id_t tile_offset, id_t tuple_id) {
  assert(tuple_id < tuple_set_container.size());

  catalog::ItemPointer item_pointer = tuple_set_container[tuple_id][tile_offset];

  storage::Tile *tile = reinterpret_cast<storage::Tile *>(catalog->locator[item_pointer.tile_id]);

  // get a copy of the tuple from the underlying physical tile
  storage::Tuple *tuple = tile->GetTuple(tuple_id);

  return tuple;
}

// Get the value from given tile at the given tuple offset and column offset
Value LogicalTile::GetValue(id_t tile_offset, id_t tuple_id, id_t column_id) {
  assert(tuple_id < tuple_set_container.size());
  assert(column_id < column_mapping.size());

  catalog::ItemPointer item_pointer = tuple_set_container[tuple_id][tile_offset];

  storage::Tile *tile = reinterpret_cast<storage::Tile *>(catalog->locator[item_pointer.tile_id]);

  // map logical tile's column id to the underlying physical tile's column id
  id_t base_tile_column_id = column_mapping[column_id].offset;

  Value value = tile->GetValue(tuple_id, base_tile_column_id);

  return value;
}


std::ostream& operator<<(std::ostream& os, const LogicalTile& logical_tile) {

  os << "\t-----------------------------------------------------------\n";

  os << "\tLOGICAL TILE\n";

  os << "\t-----------------------------------------------------------\n";
  os << "\tSCHEMA\n";
  os << (*logical_tile.schema);

  os << "\t-----------------------------------------------------------\n";
  os << "\tCOLUMN MAPPING\n";

  id_t column_count = logical_tile.column_mapping.size();
  for(id_t column_itr = 0 ; column_itr < column_count ; column_itr++) {
    os << "\t" << column_itr << " --> "
        << " { Tile : " << logical_tile.column_mapping[column_itr].tile_id
        << " Column : "<< logical_tile.column_mapping[column_itr].offset << " } \n";
  }

  os << "\t-----------------------------------------------------------\n";
  os << "\tROW MAPPING\n";

  for(auto tuple_set : logical_tile.tuple_set_container){
    os << "\t" ;
    for(auto tuple : tuple_set) {
      os << " { Tile : " << tuple.tile_id << " Tuple : " << tuple.offset << " } ";
    }
    os << "\n" ;
  }

  os << "\t-----------------------------------------------------------\n";

  return os;
}


} // End executor namespace
} // End nstore namespace
