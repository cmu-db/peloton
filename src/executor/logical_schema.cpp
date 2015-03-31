/**
 * @brief Implementation of schema for logical tiles.
 *
 * This class is operated on directly by the executors.
 * Column ids the schema uses are assumed by the entire execution engine to
 * be contiguous and zero-indexed.
 *
 * Copyright(c) 2015, CMU
 */

#include <cassert>

#include "executor/logical_schema.h"

namespace nstore {
namespace executor {

/**
 * @brief Returns base tile that column with specified id is from.
 *
 * @return Pointer to base tile.
 */
storage::Tile *LogicalSchema::GetBaseTile(id_t column_id) {
  assert(valid_bits_[column_id]);
  return base_tiles_[column_id];
}

/**
 * @brief Returns the original column id of the specified column.
 *
 * The original column id is the id of the column in the base tile.
 *
 * @return Original column id of column.
 */
id_t LogicalSchema::GetOriginColumnId(id_t column_id) {
  assert(valid_bits_[column_id]);
  return origin_columns_[column_id];
}

/**
 * @brief Adds a column to the schema.
 * @param base_tile Base tile that this column is from.
 * @param column_id Original olumn id in base tile of this column.
 */
void LogicalSchema::AddColumn(storage::Tile *base_tile, id_t column_id) {
  base_tiles_.push_back(base_tile);
  origin_columns_.push_back(column_id);
  valid_bits_.push_back(true);
  assert(base_tiles_.size() == origin_columns_.size()
      && origin_columns_.size() == valid_bits_.size());
}

/**
 * @brief Checks if specified column is valid.
 * @param column_id Id of column to check the validity of.
 * 
 * i.e. Column was not removed by projection node.
 *
 * @return True if coluimn is valid, false otherwise.
 */
bool LogicalSchema::IsValid(id_t column_id) {
  assert(column_id < valid_bits_.size());
  return valid_bits_[column_id];
}

/**
 * @brief Returns the number of columnns, including invalidated ones.
 *
 * @return Number of columns, including invalidated ones.
 */
size_t LogicalSchema::NumCols() {
  return valid_bits_.size();
}

/**
 * @brief Returns number of valid columns.
 *
 * i.e. Columns which have not been removed by projection.
 *
 * @return Number of valid columns.
 */
size_t LogicalSchema::NumValidCols() {
  size_t num_valid = 0;
  // For debugging purposes, so we don't mind that it's O(n). For now.
  for (size_t i = 0; i < valid_bits_.size(); i++) {
    if (valid_bits_[i])
      num_valid++;
  }

  return num_valid;
}

/**
 * @brief Invalidates specified column.
 *
 * Used by projection node.
 */
void LogicalSchema::InvalidateColumn(id_t column_id) {
  assert(column_id < valid_bits_.size());
  assert(valid_bits_[column_id]);
  valid_bits_[column_id] = false;
}

/** @brief Returns a string representation of this tile. */
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
