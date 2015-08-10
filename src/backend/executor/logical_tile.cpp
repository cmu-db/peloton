//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logical_tile.cpp
//
// Identification: src/backend/executor/logical_tile.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/logical_tile.h"

#include <cassert>
#include <iostream>

#include "backend/storage/tile.h"

namespace peloton {
namespace executor {

LogicalTile::~LogicalTile() {
  // Frees owned base tiles.
  for (storage::Tile *base_tile : owned_base_tiles_) {
    delete base_tile;
  }
}

/**
 * @brief Adds column metadata to the logical tile.
 * @param cp ColumnInfo that needs to be added.
 * @param own_base_tile True if the logical tile should assume ownership of
 *                      the base tile passed in.
 */
void LogicalTile::AddColumn(const ColumnInfo &cp, bool own_base_tile) {
  schema_.push_back(cp);

  if (own_base_tile) {
    owned_base_tiles_.insert(cp.base_tile);
  }
}

/**
 * @brief Get the schema of the tile.
 * @return ColumnInfo-based schema of the tile.
 */
const std::vector<LogicalTile::ColumnInfo> &LogicalTile::GetSchema() const {
  return schema_;
}

/**
 * @brief Construct the underlying physical schema of all the columns in the
 *logical tile.
 *
 * @return New schema object.
 */
catalog::Schema *LogicalTile::GetPhysicalSchema() const {
  std::vector<catalog::Column> physical_columns;

  for (ColumnInfo column : schema_) {
    auto schema = column.base_tile->GetSchema();
    auto physical_column = schema->GetColumn(column.origin_column_id);
    physical_columns.push_back(physical_column);
  }

  catalog::Schema *schema = new catalog::Schema(physical_columns);

  return schema;
}

/**
 * @brief Get the position lists of the tile.
 * @return Position lists of the tile.
 */
const std::vector<std::vector<oid_t>> &LogicalTile::GetPositionLists() const {
  return position_lists_;
}

/**
 * @brief Set the schema of the tile.
 * @param ColumnInfo-based schema of the tile.
 */
void LogicalTile::SetSchema(std::vector<LogicalTile::ColumnInfo> &&schema) {
  schema_ = schema;
}

/**
 * @brief Set the position lists of the tile.
 * @param Position lists.
 */
void LogicalTile::SetPositionLists(
    std::vector<std::vector<oid_t>> &&position_lists) {
  position_lists_ = position_lists;
}

void LogicalTile::SetPositionListsAndVisibility(
    std::vector<std::vector<oid_t>> &&position_lists) {
  position_lists_ = position_lists;
  if (position_lists.size() > 0) {
    visible_rows_.resize(position_lists_[0].size(), true);
    num_tuples_ = position_lists_[0].size();
  }
}

/**
 * @brief Transfer all owned basetiles to another logical tile
 *        and give up all the base tiles
 *
 */
void LogicalTile::TransferOwnershipTo(LogicalTile *other) {
  auto other_ownership_set = other->GetOwnedBaseTiles();
  other_ownership_set.insert(owned_base_tiles_.begin(), owned_base_tiles_.end());
  owned_base_tiles_.clear();
}

/**
 * @brief Get the underlying owned base tile set
 * @return Owned base tile set of the tile
 */
std::unordered_set<storage::Tile *> &LogicalTile::GetOwnedBaseTiles() {
  return owned_base_tiles_;
}




/**
 * @brief Adds column metadata to the logical tile.
 * @param base_tile Base tile that this column is from.
 * @param own_base_tile True if the logical tile should assume ownership of
 *                      the base tile passed in.
 * @param origin_column_id Original column id of this column in its base tile.
 * @param position_list_idx Index of the position list corresponding to this
 *        column.
 *
 * The position list corresponding to this column should be added
 * before the metadata.
 */
void LogicalTile::AddColumn(storage::Tile *base_tile, bool own_base_tile,
                            oid_t origin_column_id, oid_t position_list_idx) {
  assert(position_list_idx < position_lists_.size());

  ColumnInfo cp;
  cp.base_tile = base_tile;
  cp.origin_column_id = origin_column_id;
  cp.position_list_idx = position_list_idx;
  schema_.push_back(cp);

  if (own_base_tile) {
    owned_base_tiles_.insert(base_tile);
  }
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
int LogicalTile::AddPositionList(std::vector<oid_t> &&position_list) {
  assert(position_lists_.size() == 0 ||
         position_lists_[0].size() == position_list.size());

  if (position_lists_.size() == 0) {
    num_tuples_ = position_list.size();
    visible_rows_.resize(position_list.size(), true);
  }

  position_lists_.push_back(std::move(position_list));
  return position_lists_.size() - 1;
}

/**
 * @brief Remove visibility the specified tuple in the logical tile.
 * @param tuple_id Id of the specified tuple.
 */
void LogicalTile::RemoveVisibility(oid_t tuple_id) {
  assert(tuple_id < visible_rows_.size());
  assert(visible_rows_[tuple_id]);

  visible_rows_[tuple_id] = false;
  num_tuples_--;
}

/**
 * @brief Returns base tile that the specified column was from.
 * @param column_id Id of the specified column.
 *
 * @return Pointer to base tile of specified column.
 */
storage::Tile *LogicalTile::GetBaseTile(oid_t column_id) {
  return schema_[column_id].base_tile;
}

/**
 * @brief Get the value at the specified field.
 * @param tuple_id Tuple id of the specified field (row/position).
 * @param column_id Column id of the specified field.
 *
 * @return Value at the specified field,
 *         or VALUE_TYPE_INVALID if it doesn't exist.
 */
// TODO Amortize schema lookups by using iterator instead?
Value LogicalTile::GetValue(oid_t tuple_id, oid_t column_id) {
  assert(column_id < schema_.size());
  assert(tuple_id < visible_rows_.size());
  assert(visible_rows_[tuple_id]);

  ColumnInfo &cp = schema_[column_id];
  oid_t base_tuple_id = position_lists_[cp.position_list_idx][tuple_id];
  storage::Tile *base_tile = cp.base_tile;

  LOG_TRACE("Tuple : %u Column : %u", base_tuple_id, cp.origin_column_id);
  Value value = base_tile->GetValue(base_tuple_id, cp.origin_column_id);

  return value;
}

/**
 * @brief Returns the number of visible tuples in this logical tile.
 *
 * @return Number of tuples.
 */
size_t LogicalTile::GetTupleCount() { return num_tuples_; }

/**
 * @brief Returns the number of columns.
 *
 * @return Number of columns.
 */
size_t LogicalTile::GetColumnCount() { return schema_.size(); }

/**
 * @brief Returns iterator pointing to first tuple.
 *
 * @return iterator pointing to first tuple.
 */
LogicalTile::iterator LogicalTile::begin() {
  bool begin = true;
  return iterator(this, begin);
}

/**
 * @brief Returns iterator indicating that we are past the last tuple.
 *
 * @return iterator indicating we're past the last tuple.
 */
LogicalTile::iterator LogicalTile::end() {
  bool begin = false;
  return iterator(this, begin);
}

/**
 * @brief Constructor for iterator.
 * @param Logical tile corresponding to this iterator.
 * @param begin Specifies whether we want the iterator initialized to point
 *              to the first tuple id, or to past-the-last tuple.
 */
LogicalTile::iterator::iterator(LogicalTile *tile, bool begin) : tile_(tile) {
  if (!begin) {
    pos_ = INVALID_OID;
    return;
  }

  // Find first visible tuple.
  pos_ = 0;
  while (pos_ < tile_->visible_rows_.size() && !tile_->visible_rows_[pos_]) {
    pos_++;
  }

  // If no visible tuples...
  if (pos_ == tile_->visible_rows_.size()) {
    pos_ = INVALID_OID;
  }
}

/**
 * @brief Increment operator.
 *
 * It ignores invisible tuples.
 *
 * @return iterator after the increment.
 */
LogicalTile::iterator &LogicalTile::iterator::operator++() {
  // Find next visible tuple.
  do {
    pos_++;
  } while (pos_ < tile_->visible_rows_.size() && !tile_->visible_rows_[pos_]);

  if (pos_ == tile_->visible_rows_.size()) {
    pos_ = INVALID_OID;
  }

  return *this;
}

/**
 * @brief Increment operator.
 *
 * It ignores invisible tuples.
 *
 * @return iterator before the increment.
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
oid_t LogicalTile::iterator::operator*() { return pos_; }

/** @brief Returns a string representation of this tile. */
std::ostream &operator<<(std::ostream &os, const LogicalTile &lt) {
  os << "\t-----------------------------------------------------------\n";

  os << "\tLOGICAL TILE\n";

  os << "\t-----------------------------------------------------------\n";
  os << "\t SCHEMA : \n";
  for (unsigned int i = 0; i < lt.schema_.size(); i++) {
    const LogicalTile::ColumnInfo &cp = lt.schema_[i];
    os << "\t Position list idx: " << cp.position_list_idx << ", "
       << "base tile: " << cp.base_tile << ", "
       << "origin column id: " << cp.origin_column_id << std::endl;
  }

  os << "\t-----------------------------------------------------------\n";
  os << "\t VISIBLE ROWS : ";

  for (unsigned int i = 0; i < lt.visible_rows_.size(); i++) {
    os << lt.visible_rows_[i] << " ";
  }

  os << std::endl;

  os << "\t-----------------------------------------------------------\n";
  os << "\t POSITION LISTS : \n";

  int pos_list_id = 0;
  for (auto position_list : lt.position_lists_) {
    os << "\t " << pos_list_id++ << " : ";
    for (auto pos : position_list) {
      os << pos << " ";
    }
    os << "\n";
  }

  os << "\t-----------------------------------------------------------\n";

  return os;
}

}  // End executor namespace
}  // End peloton namespace
