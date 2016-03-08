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


#include <cassert>
#include <iostream>

#include "backend/catalog/schema.h"
#include "backend/common/value.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile.h"
#include "backend/common/value_factory.h"
#include "backend/executor/logical_tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Get the schema of the tile.
 * @return ColumnInfo-based schema of the tile.
 */
const std::vector<LogicalTile::ColumnInfo> &LogicalTile::GetSchema() const {
  return schema_;
}

/**
 * @brief Get the information about the column.
 * @return ColumnInfo of the column.
 */
const LogicalTile::ColumnInfo &LogicalTile::GetColumnInfo(
    const oid_t column_id) const {
  return schema_[column_id];
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
const LogicalTile::PositionLists &LogicalTile::GetPositionLists() const {
  return position_lists_;
}

/**
 * @brief Get the position list at given offset in the tile.
 * @return Position list associated with column.
 */
const LogicalTile::PositionList &LogicalTile::GetPositionList(
    const oid_t column_id) const {
  return position_lists_[column_id];
}

/**
 * @brief Set the position lists of the tile.
 * @param Position lists.
 */
void LogicalTile::SetPositionLists(
    LogicalTile::PositionLists &&position_lists) {
  position_lists_ = position_lists;
}

void LogicalTile::SetPositionListsAndVisibility(
    LogicalTile::PositionLists &&position_lists) {
  position_lists_ = position_lists;
  if (position_lists.size() > 0) {
    total_tuples_ = position_lists[0].size();
    visible_rows_.resize(position_lists_[0].size(), true);
    visible_tuples_ = position_lists_[0].size();
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
int LogicalTile::AddPositionList(LogicalTile::PositionList &&position_list) {
  assert(position_lists_.size() == 0 ||
         position_lists_[0].size() == position_list.size());

  if (position_lists_.size() == 0) {
    visible_tuples_ = position_list.size();
    visible_rows_.resize(position_list.size(), true);

    // All tuples are visible initially
    total_tuples_ = visible_tuples_;
  }

  position_lists_.push_back(std::move(position_list));
  return position_lists_.size() - 1;
}

/**
 * @brief Remove visibility the specified tuple in the logical tile.
 * @param tuple_id Id of the specified tuple.
 */
void LogicalTile::RemoveVisibility(oid_t tuple_id) {
  assert(tuple_id < total_tuples_);
  assert(visible_rows_[tuple_id]);

  visible_rows_[tuple_id] = false;
  visible_tuples_--;
}

/**
 * @brief Returns base tile that the specified column was from.
 * @param column_id Id of the specified column.
 *
 * @return Pointer to base tile of specified column.
 */
storage::Tile *LogicalTile::GetBaseTile(oid_t column_id) {
  return schema_[column_id].base_tile.get();
}

/**
 * @brief Get the value at the specified field.
 * @param tuple_id Tuple id of the specified field (row/position).
 * @param column_id Column id of the specified field.
 *
 * @return Value at the specified field,
 *         or VALUE_TYPE_INVALID if it doesn't exist.
 */
// TODO: Deprecated. Avoid calling this function if possible.
Value LogicalTile::GetValue(oid_t tuple_id, oid_t column_id) {
  assert(column_id < schema_.size());
  assert(tuple_id < total_tuples_);
  assert(visible_rows_[tuple_id]);

  ColumnInfo &cp = schema_[column_id];
  oid_t base_tuple_id = position_lists_[cp.position_list_idx][tuple_id];
  storage::Tile *base_tile = cp.base_tile.get();

  LOG_TRACE("Tuple : %lu Column : %lu", base_tuple_id, cp.origin_column_id);
  if (base_tuple_id == NULL_OID) {
    return ValueFactory::GetNullValueByType(
        base_tile->GetSchema()->GetType(column_id));
  } else {
    return base_tile->GetValue(base_tuple_id, cp.origin_column_id);
  }
}

/**
 * @brief Returns the number of visible tuples in this logical tile.
 *
 * @return Number of tuples.
 */
size_t LogicalTile::GetTupleCount() { return visible_tuples_; }

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

  auto total_tile_tuples = tile_->total_tuples_;

  // Find first visible tuple.
  pos_ = 0;
  while (pos_ < total_tile_tuples && !tile_->visible_rows_[pos_]) {
    pos_++;
  }

  // If no visible tuples...
  if (pos_ == total_tile_tuples) {
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
  auto total_tile_tuples = tile_->total_tuples_;

  // Find next visible tuple.
  do {
    pos_++;
  } while (pos_ < total_tile_tuples && !tile_->visible_rows_[pos_]);

  if (pos_ == total_tile_tuples) {
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

LogicalTile::~LogicalTile() {
  // Automatically drops reference on base tiles for each column
}

LogicalTile::PositionListsBuilder::PositionListsBuilder() {
  // Nothing to do here !
}

LogicalTile::PositionListsBuilder::PositionListsBuilder(LogicalTile *left_tile,
                                                        LogicalTile *right_tile)
: left_source_(&left_tile->GetPositionLists()),
  right_source_(&right_tile->GetPositionLists()) {
  // Compute the output logical tile column count
  size_t left_tile_column_count = left_source_->size();
  size_t right_tile_column_count = right_source_->size();
  size_t output_tile_column_count =
      left_tile_column_count + right_tile_column_count;

  assert(left_tile_column_count > 0);
  assert(right_tile_column_count > 0);

  // Construct position lists for output tile
  for (size_t column_itr = 0; column_itr < output_tile_column_count;
      column_itr++) {
    output_lists_.push_back(std::vector<oid_t>());
  }
}

/**
 * @brief Set the schema of the tile.
 * @param ColumnInfo-based schema of the tile.
 */
void LogicalTile::SetSchema(std::vector<LogicalTile::ColumnInfo> &&schema) {
  schema_ = schema;
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
void LogicalTile::AddColumn(const std::shared_ptr<storage::Tile> &base_tile,
                            oid_t origin_column_id, oid_t position_list_idx) {
  ColumnInfo cp;

  // Add a reference to the base tile
  cp.base_tile = base_tile;

  cp.origin_column_id = origin_column_id;
  cp.position_list_idx = position_list_idx;
  schema_.push_back(cp);
}

/**
 * @brief Add the column specified in column_ids to this logical tile.
 */
void LogicalTile::AddColumns(
    const std::shared_ptr<storage::TileGroup> &tile_group,
    const std::vector<oid_t> &column_ids) {
  const int position_list_idx = 0;
  for (oid_t origin_column_id : column_ids) {
    oid_t base_tile_offset, tile_column_id;

    tile_group->LocateTileAndColumn(origin_column_id, base_tile_offset,
                                    tile_column_id);

    AddColumn(tile_group->GetTileReference(base_tile_offset), tile_column_id,
              position_list_idx);
  }
}

/**
 * @brief Given the original column ids, reorganize the schema to conform the
 * new column_ids
 * column_ids is a vector of oid_t. Each column_id is the index into the
 * original table schema
 * schema_ is a vector of ColumnInfos. Each ColumnInfo represents a column in
 * the corresponding place in colum_ids.
 */
void LogicalTile::ProjectColumns(const std::vector<oid_t> &original_column_ids,
                                 const std::vector<oid_t> &column_ids) {
  std::vector<ColumnInfo> new_schema;
  for (auto id : column_ids) {
    auto ret =
        std::find(original_column_ids.begin(), original_column_ids.end(), id);
    assert(ret != original_column_ids.end());
    new_schema.push_back(schema_[*ret]);
  }

  // remove references to base tiles from columns that are projected away
  schema_ = std::move(new_schema);
}

const std::string LogicalTile::GetInfo() const {
  std::ostringstream os;
  os << "\t-----------------------------------------------------------\n";

  os << "\tLOGICAL TILE\n";

  os << "\t-----------------------------------------------------------\n";
  os << "\t VALUES : \n";

  for (oid_t tuple_itr = 0; tuple_itr < total_tuples_; tuple_itr++) {

    if(visible_rows_[tuple_itr] == false)
      continue;

    os << "\t";

    for (oid_t column_itr = 0; column_itr < schema_.size(); column_itr++) {
      const LogicalTile::ColumnInfo &cp = schema_[column_itr];

      oid_t base_tuple_id = position_lists_[cp.position_list_idx][tuple_itr];

      if (base_tuple_id == NULL_OID) {
        os << ValueFactory::GetNullValueByType(
            cp.base_tile->GetSchema()->GetType(cp.origin_column_id)) << " ";
      } else {
        os << cp.base_tile->GetValue(base_tuple_id, cp.origin_column_id) << " ";
      }
    }

    os << "\n";
  }

  os << "\t-----------------------------------------------------------\n";

  return os.str();
}

}  // End executor namespace
}  // End peloton namespace
