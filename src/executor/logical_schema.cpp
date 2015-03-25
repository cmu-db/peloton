/*-------------------------------------------------------------------------
 *
 * logical_schema.cpp
 * Schema for logical tile.
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/logical_schema.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>

#include "executor/logical_schema.h"

namespace nstore {
namespace executor {

storage::Tile *LogicalSchema::GetBaseTile(id_t column_id) {
  assert(valid_bits[column_id]);
  return base_tiles[column_id];
}

id_t LogicalSchema::GetOriginColumnId(id_t column_id) {
  assert(valid_bits[column_id]);
  return origin_columns[column_id];
}

void LogicalSchema::AddColumn(storage::Tile *base_tile, id_t column_id) {
  base_tiles.push_back(base_tile);
  origin_columns.push_back(column_id);
  valid_bits.push_back(true);
}

bool LogicalSchema::IsValid(id_t column_id) {
  assert(column_id < valid_bits.size());
  return valid_bits[column_id];
}

size_t LogicalSchema::NumCols() {
  return valid_bits.size();
}

std::ostream& operator<< (std::ostream& os, const LogicalSchema& schema) {
  os << "\tLogical Schema:\n";

  for (id_t idx = 0; idx < schema.base_tiles.size(); idx++) {
    os << "\t Column " << idx << " :: ";
    if (!schema.valid_bits[idx]) {
      os << "(INVALIDATED) ";
    }
    os << "base tile " <<  schema.base_tiles[idx] << ", ";
    os << "origin column " << schema.origin_columns[idx];
    os << std::endl;
  }

  return os;
}

} // namespace executor
} // namespace nstore
