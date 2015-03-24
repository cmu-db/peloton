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

oid_t LogicalSchema::GetBaseTileOid(id_t column_id) {
  assert(valid_bits[column_id]);
  return base_tiles[column_id];
}

id_t LogicalSchema::GetOriginColumnId(id_t column_id) {
  assert(valid_bits[column_id]);
  return origin_columns[column_id];
}

std::ostream& operator<<(std::ostream& os, const LogicalSchema& schema) {
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
}

} // namespace executor
} // namespace nstore
