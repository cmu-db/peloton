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
  assert(valid_bits_[column_id]);
  return base_tiles_[column_id];
}

id_t LogicalSchema::GetOriginColumnId(id_t column_id) {
  assert(valid_bits_[column_id]);
  return origin_columns_[column_id];
}

void LogicalSchema::AddColumn(storage::Tile *base_tile, id_t column_id) {
  base_tiles_.push_back(base_tile);
  origin_columns_.push_back(column_id);
  valid_bits_.push_back(true);
  assert(base_tiles_.size() == origin_columns_.size()
      && origin_columns_.size() == valid_bits_.size());
}

bool LogicalSchema::IsValid(id_t column_id) {
  assert(column_id < valid_bits_.size());
  return valid_bits_[column_id];
}

size_t LogicalSchema::NumCols() {
  return valid_bits_.size();
}

size_t LogicalSchema::NumValidCols() {
  size_t num_valid = 0;
  // For debugging purposes, so we don't mind that it's O(n). For now.
  for (size_t i = 0; i < valid_bits_.size(); i++) {
    if (valid_bits_[i])
      num_valid++;
  }

  return num_valid;
}

void LogicalSchema::InvalidateColumn(id_t column_id) {
  assert(column_id < valid_bits_.size());
  assert(valid_bits_[column_id]);
  valid_bits_[column_id] = false;
}

std::ostream& operator<< (std::ostream& os, const LogicalSchema& schema) {
  os << "\tLogical Schema:\n";

  for (id_t idx = 0; idx < schema.base_tiles_.size(); idx++) {
    os << "\t Column " << idx << " :: ";
    if (!schema.valid_bits_[idx]) {
      os << "(INVALIDATED) ";
    }
    os << "base tile " <<  schema.base_tiles_[idx] << ", ";
    os << "origin column " << schema.origin_columns_[idx];
    os << std::endl;
  }

  return os;
}

} // namespace executor
} // namespace nstore
