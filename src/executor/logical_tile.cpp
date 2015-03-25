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

// Add a position tuple to the container.
void LogicalTile::AppendPositionTuple(std::vector<id_t> tuple) {
  assert(tuple.size() <= schema->NumCols());
  // First we ensure that the columns of the position tuple align with the schema.
  // (Because some columns might exist but be invalidated)
  std::vector<id_t> aligned_tuple;
  int tuple_idx = 0;
  for (unsigned int schema_idx = 0; schema_idx < schema->NumCols(); schema_idx++) {
    if (schema->IsValid(schema_idx)) {
      aligned_tuple.push_back(tuple[tuple_idx]);
      tuple_idx++;
    } else {
      aligned_tuple.push_back(INVALID_ID);
    }
  }

  // Add aligned tuple to tuple list.
  position_tuple_list.push_back(aligned_tuple);
}

// Get the tuple from given tile at the given tuple offset
storage::Tuple *LogicalTile::GetTuple(id_t column_id, id_t tuple_id) {
  assert(tuple_id < position_tuple_list.size());
  assert(schema->IsValid(column_id));

  id_t base_tuple_id = position_tuple_list[tuple_id][column_id];
  storage::Tile *base_tile = schema->GetBaseTile(column_id);

  // get a copy of the tuple from the underlying physical tile
  storage::Tuple *tuple = base_tile->GetTuple(base_tuple_id);

  return tuple;
}

// Get the value from given tile at the given tuple offset and column offset
Value LogicalTile::GetValue(id_t column_id, id_t tuple_id) {
  assert(tuple_id < position_tuple_list.size());
  assert(schema->IsValid(column_id));

  id_t base_tuple_id = position_tuple_list[tuple_id][column_id];
  storage::Tile *base_tile = schema->GetBaseTile(column_id);
  id_t base_column_id = schema->GetOriginColumnId(column_id);

  Value value = base_tile->GetValue(base_tuple_id, base_column_id);

  return value;
}

std::ostream& operator<<(std::ostream& os, const LogicalTile& logical_tile) {

  os << "\t-----------------------------------------------------------\n";

  os << "\tLOGICAL TILE\n";

  os << "\t-----------------------------------------------------------\n";
  os << "\tSCHEMA\n";
  os << (*logical_tile.schema);

  os << "\t-----------------------------------------------------------\n";
  os << "\tROW MAPPING\n";

  for(auto position_tuple : logical_tile.position_tuple_list){
    os << "\t" ;
    for(auto pos : position_tuple) {
      os << " Position: " << pos << ", ";
    }
    os << "\n" ;
  }

  os << "\t-----------------------------------------------------------\n";

  return os;
}


} // End executor namespace
} // End nstore namespace
