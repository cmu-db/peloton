/**
 * @brief Implementation of logical tile.
 *
 * This abstraction is used to implement late materialization of tiles in the
 * execution engine.
 * Tiles are only instantiated via LogicalTileFactory.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/logical_tile.h"

#include <cassert>
#include <iostream>

#include "common/value_factory.h"
#include "storage/tile.h"

namespace nstore {
namespace executor {

/**
 * @brief Adds column metadata to the logical tile.
 * @param base_tile Base tile that this column is from.
 * @param origin_column_id Original column id of this column in its base tile.
 * @param position_list_idx Index of the position list corresponding to this
 *        column.
 *
 * The position list corresponding to this column should be added
 * before the metadata.
 */
void LogicalTile::AddColumn(
    storage::Tile *base_tile,
    id_t origin_column_id,
    unsigned int position_list_idx) {
  assert(position_list_idx < position_lists_.size());

  ColumnPointer cp;
  cp.base_tile = base_tile;
  cp.origin_column_id = origin_column_id;
  cp.position_list_idx = position_list_idx;
  schema_.push_back(cp);
}

/**
 * @brief Adds position list to logical tile.
 * @param position_list Position list to be added. Note the move semantics.
 *
 * The first position list to be added determines the number of rows in this
 * logical tile.
 *
 * @return Position list index of newly added list.
 */
int LogicalTile::AddPositionList(std::vector<id_t> &&position_list) {
  assert(position_lists_.size() == 0
      || position_lists_[0].size() == position_list.size());

  if (position_lists_.size() == 0) {
    valid_rows_.resize(position_list.size(), true);
  }
  position_lists_.push_back(std::move(position_list));
  return position_lists_.size() - 1;
}

/**
 * @brief Returns base tile that the specified column was from.
 * @param column_id Id of the specified column.
 *
 * @return Pointer to base tile of specified column.
 */
storage::Tile *LogicalTile::GetBaseTile(id_t column_id) {
  return schema_[column_id].base_tile;
}

/**
 * @brief Get the tuple from the base tile that contains the specified field.
 * @param column_id Column id of the specified field.
 * @param tuple_id Tuple id of the specified field (row/position).
 *
 * @return Pointer to copy of tuple from base tile.
 */
storage::Tuple *LogicalTile::GetTuple(id_t column_id, id_t tuple_id) {
  assert(column_id < schema_.size());
  assert(tuple_id < valid_rows_.size());

  if (!valid_rows_[tuple_id]) {
    return NULL;
  }

  ColumnPointer &cp = schema_[column_id];
  id_t base_tuple_id = position_lists_[cp.position_list_idx][tuple_id];
  storage::Tile *base_tile = cp.base_tile;

  // Get a copy of the tuple from the underlying physical tile.
  storage::Tuple *tuple = base_tile->GetTuple(base_tuple_id);

  return tuple;
}

/**
 * @brief Get the value at the specified field.
 * @param column_id Column id of the specified field.
 * @param tuple_id Tuple id of the specified field (row/position).
 *
 * @return Value at the specified field,
 *         or VALUE_TYPE_INVALID if it doesn't exist.
 */
Value LogicalTile::GetValue(id_t column_id, id_t tuple_id) {
  assert(column_id < schema_.size());
  assert(tuple_id < valid_rows_.size());

  if (!valid_rows_[tuple_id]) {
    return ValueFactory::GetInvalidValue();
  }

  ColumnPointer &cp = schema_[column_id];
  id_t base_tuple_id = position_lists_[cp.position_list_idx][tuple_id];
  storage::Tile *base_tile = cp.base_tile;

  Value value = base_tile->GetValue(base_tuple_id, cp.origin_column_id);

  return value;
}

/**
 * @brief Returns the number of tuples in this logical tile.
 *
 * @return Number of tuples.
 */
int LogicalTile::NumTuples() {
  //TODO WRONG! We want number of valid tuples.
  assert(position_lists_.size() == 0
      || valid_rows_.size() == position_lists_[0].size());
  return valid_rows_.size();
}

/**
 * @brief Constructor for iterator.
 * @param Logical tile corresponding to this iterator.
 */
LogicalTile::iterator::iterator(LogicalTile *tile)
  : pos_(0),
    tile_(tile) {
}

/**
 * @brief Increment operator.
 *
 * It ignores invalidated tuples.
 *
 * @return Iterator after the increment.
 */
LogicalTile::iterator &LogicalTile::iterator::operator++() {
  do {
    pos_++;
  } while(pos_ < tile_->valid_rows_.size() && !tile_->valid_rows_[pos_]);

  return *this;
}

/**
 * @brief Increment operator.
 *
 * It ignores invalidated tuples.
 *
 * @return Iterator before the increment.
 */
LogicalTile::iterator LogicalTile::iterator::operator++(int) {
  LogicalTile::iterator tmp(*this);
  operator++();
  return tmp;
}

/**
 * @brief Equality operator.
 * @param rhs The iterator to compare to.
 *
 * @return True if equal, false otherwise.
 */
bool LogicalTile::iterator::operator==(const iterator &rhs) {
  return pos_ == rhs.pos_ && tile_ == rhs.tile_;
}

/**
 * @brief Inequality operator.
 * @param rhs The iterator to compare to.
 *
 * @return False if equal, true otherwise.
 */
bool LogicalTile::iterator::operator!=(const iterator &rhs) {
  return pos_ != rhs.pos_ || tile_ != rhs.tile_;
}

/**
 * @brief Dereference operator.
 *
 * @return Id of tuple that iterator is pointing at.
 */
id_t LogicalTile::iterator::operator*() {
  return pos_;
}

/** @brief Returns a string representation of this tile. */
std::ostream& operator<<(std::ostream& os, const LogicalTile& lt) {

  os << "\t-----------------------------------------------------------\n";

  os << "\tLOGICAL TILE\n";

  os << "\t-----------------------------------------------------------\n";
  os << "\tSCHEMA\n";
  for (unsigned int i = 0; i < lt.schema_.size(); i++) {
    const LogicalTile::ColumnPointer &cp = lt.schema_[i]; 
    os << "Position list idx: " << cp.position_list_idx << ", "
       << "base tile: " << cp.base_tile << ", "
       << "origin column id: " << cp.origin_column_id << std::endl;
  }

  os << "\t-----------------------------------------------------------\n";
  os << "\tVALID ROWS\n";

  for (unsigned int i = 0; i < lt.valid_rows_.size(); i++) {
    os << lt.valid_rows_[i] << ", ";
  }

  os << std::endl;

  os << "\t-----------------------------------------------------------\n";
  os << "\tPOSITION LISTS\n";

  for(auto position_list : lt.position_lists_){
    os << "\t" ;
    for(auto pos : position_list) {
      os << pos << ", ";
    }
    os << "\n" ;
  }

  os << "\t-----------------------------------------------------------\n";

  return os;
}

} // End executor namespace
} // End nstore namespace
