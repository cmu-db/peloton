//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logical_tile.h
//
// Identification: src/backend/executor/logical_tile.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iterator>
#include <unordered_set>
#include <vector>

#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/storage/tile_group.h"

namespace peloton {

namespace storage {
class Tile;
class Tuple;
}

namespace executor {

//===--------------------------------------------------------------------===//
// Logical Tile
//===--------------------------------------------------------------------===//

/**
 * Represents a Logical Tile that can represent columns in many Physical Tiles.
 *
 * LT :: <C1, C2>
 * C1 :: col 5 in PT 5
 * C2 :: col 3 in PT 9 ...
 *
 * LogicalTiles are only instantiated via LogicalTileFactory.
 */
class LogicalTile {
  friend class LogicalTileFactory;

 public:
  struct ColumnInfo;

  LogicalTile(const LogicalTile &) = delete;
  LogicalTile &operator=(const LogicalTile &) = delete;
  LogicalTile(LogicalTile &&) = delete;
  LogicalTile &operator=(LogicalTile &&) = delete;

  ~LogicalTile();

  inline void AddColumn(const ColumnInfo &cp, bool own_base_tile);

  inline void AddColumn(storage::Tile *base_tile, bool own_base_tile,
                 oid_t origin_column_id, oid_t position_list_idx);

  inline void AddColumns(storage::TileGroup *tile_group, const std::vector<oid_t> &column_ids);

  inline void ProjectColumns(const std::vector<oid_t> &original_column_ids, const std::vector<oid_t> &column_ids);

  inline int AddPositionList(std::vector<oid_t> &&position_list);

  inline void RemoveVisibility(oid_t tuple_id);

  inline storage::Tile *GetBaseTile(oid_t column_id);

  inline Value GetValue(oid_t tuple_id, oid_t column_id);

  inline size_t GetTupleCount();

  inline size_t GetColumnCount();

  inline const std::vector<ColumnInfo> &GetSchema() const;

  inline const ColumnInfo &GetColumnInfo(const oid_t column_id) const;

  inline catalog::Schema *GetPhysicalSchema() const;

  inline void SetSchema(std::vector<LogicalTile::ColumnInfo> &&schema);

  inline const std::vector<std::vector<oid_t>> &GetPositionLists() const;

  inline const std::vector<oid_t> &GetPositionList(const oid_t column_id) const;

  inline void SetPositionLists(std::vector<std::vector<oid_t>> &&position_lists);

  inline void SetPositionListsAndVisibility(
      std::vector<std::vector<oid_t>> &&position_lists);

  inline void TransferOwnershipTo(LogicalTile *other);

  inline std::unordered_set<storage::Tile *> &GetOwnedBaseTiles();

  friend std::ostream &operator<<(std::ostream &os,
                                  const LogicalTile &logical_tile);

  //===--------------------------------------------------------------------===//
  // Logical Tile Iterator
  //===--------------------------------------------------------------------===//

  /**
   * @brief Iterates through tuple ids in this logical tile.
   *
   * This provides easy access to tuples that are visible.
   */
  class iterator : public std::iterator<std::input_iterator_tag, oid_t> {
    // It's a friend so it can call this iterator's private constructor.
    friend class LogicalTile;

   public:
    inline iterator &operator++();

    inline iterator operator++(int);

    inline bool operator==(const iterator &rhs);

    inline bool operator!=(const iterator &rhs);

    inline oid_t operator*();

   private:
    inline iterator(LogicalTile *tile, bool begin);

    /** @brief Keeps track of position of iterator. */
    oid_t pos_;

    /** @brief Tile that this iterator is iterating over. */
    LogicalTile *tile_;
  };

  inline iterator begin();

  inline iterator end();

  //===--------------------------------------------------------------------===//
  // Column Info
  //===--------------------------------------------------------------------===//

  /** @brief Column metadata for logical tile */
  struct ColumnInfo {
    /** @brief Position list in logical tile that will correspond to this
     * column. */
    oid_t position_list_idx;

    /**
     * @brief Pointer to base tile that column is from.
     * IMPORTANT: We use a pointer instead of the oid of the tile to minimize
     * indirection.
     */
    storage::Tile *base_tile;

    /** @brief Original column id of this logical tile column in its associated
     * base tile. */
    oid_t origin_column_id;
  };

 private:
  // Dummy default constructor
  LogicalTile(){};

  /**
   * @brief Mapping of column ids in this logical tile to the underlying
   *        position lists and columns in base tiles.
   */
  std::vector<ColumnInfo> schema_;

  /**
   * @brief Lists of position lists.
   * Each list contains positions corresponding to particular tiles/columns.
   */
  std::vector<std::vector<oid_t>> position_lists_;

  /**
   * @brief Bit-vector storing visibility of each row in the position lists.
   * Used to cheaply invalidate rows of positions.
   */
  std::vector<bool> visible_rows_;

  /** @brief Total # of allocated slots in the logical tile **/
  oid_t total_tuples_ = 0;

  /** @brief Keeps track of the number of tuples that are still visible. */
  oid_t visible_tuples_ = 0;

  /** @brief Set of base tiles owned (memory-wise) by this logical tile. */
  std::unordered_set<storage::Tile *> owned_base_tiles_;
};

/**
 * @brief Adds column metadata to the logical tile.
 * @param cp ColumnInfo that needs to be added.
 * @param own_base_tile True if the logical tile should assume ownership of
 *                      the base tile passed in.
 */
inline void LogicalTile::AddColumn(const ColumnInfo &cp, bool own_base_tile) {
  schema_.push_back(cp);

  if (own_base_tile) {
    owned_base_tiles_.insert(cp.base_tile);
  }
}

/**
 * @brief Get the schema of the tile.
 * @return ColumnInfo-based schema of the tile.
 */
inline const std::vector<LogicalTile::ColumnInfo> &LogicalTile::GetSchema() const {
  return schema_;
}

/**
 * @brief Get the information about the column.
 * @return ColumnInfo of the column.
 */
inline const LogicalTile::ColumnInfo &LogicalTile::GetColumnInfo(const oid_t column_id) const {
  return schema_[column_id];
}

/**
 * @brief Construct the underlying physical schema of all the columns in the
 *logical tile.
 *
 * @return New schema object.
 */
inline catalog::Schema *LogicalTile::GetPhysicalSchema() const {
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
inline const std::vector<std::vector<oid_t>> &LogicalTile::GetPositionLists() const {
  return position_lists_;
}

/**
 * @brief Get the position list at given offset in the tile.
 * @return Position list associated with column.
 */
inline const std::vector<oid_t> &LogicalTile::GetPositionList(const oid_t column_id) const {
  return position_lists_[column_id];
}

/**
 * @brief Set the schema of the tile.
 * @param ColumnInfo-based schema of the tile.
 */
inline void LogicalTile::SetSchema(std::vector<LogicalTile::ColumnInfo> &&schema) {
  schema_ = schema;
}

/**
 * @brief Set the position lists of the tile.
 * @param Position lists.
 */
inline void LogicalTile::SetPositionLists(
    std::vector<std::vector<oid_t>> &&position_lists) {
  position_lists_ = position_lists;
}

inline void LogicalTile::SetPositionListsAndVisibility(
    std::vector<std::vector<oid_t>> &&position_lists) {
  position_lists_ = position_lists;
  if (position_lists.size() > 0) {
    visible_rows_.resize(position_lists_[0].size(), true);
    visible_tuples_ = position_lists_[0].size();
  }
}

/**
 * @brief Transfer all owned basetiles to another logical tile
 *        and give up all the base tiles
 *
 */
inline void LogicalTile::TransferOwnershipTo(LogicalTile *other) {
  auto other_ownership_set = other->GetOwnedBaseTiles();
  other_ownership_set.insert(owned_base_tiles_.begin(),
                             owned_base_tiles_.end());
  owned_base_tiles_.clear();
}

/**
 * @brief Get the underlying owned base tile set
 * @return Owned base tile set of the tile
 */
inline std::unordered_set<storage::Tile *> &LogicalTile::GetOwnedBaseTiles() {
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
inline void LogicalTile::AddColumn(storage::Tile *base_tile, bool own_base_tile,
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
 * @brief Add the column specified in column_ids to this logical tile.
 */
inline void LogicalTile::AddColumns(storage::TileGroup *tile_group, const std::vector<oid_t> &column_ids) {
  const int position_list_idx = 0;
  const bool own_base_tile = false;
  for (oid_t origin_column_id : column_ids) {
    oid_t base_tile_offset, tile_column_id;

    tile_group->LocateTileAndColumn(origin_column_id, base_tile_offset,
                                    tile_column_id);

    AddColumn(tile_group->GetTile(base_tile_offset),
                            own_base_tile, tile_column_id, position_list_idx);
  }
}


/**
 * @brief Given the original column ids, reorganize the schema to conform the new column_ids
 * column_ids is a vector of oid_t. Each column_id is the index into the original table schema
 * schema_ is a vector of ColumnInfos. Each ColumnInfo represents a column in the corresponding place in colum_ids.
 */
inline void LogicalTile::ProjectColumns(const std::vector<oid_t> &original_column_ids, const std::vector<oid_t> &column_ids) {
  std::vector<ColumnInfo> new_schema;
  for (auto id : column_ids) {
    auto ret = std::find(original_column_ids.begin(), original_column_ids.end(), id);
    assert(ret != original_column_ids.end());
    new_schema.push_back(schema_[*ret]);
  }

  // remove ownership if needed
  for (auto it = owned_base_tiles_.begin(); it != owned_base_tiles_.end();) {
    for (auto cp : new_schema) {
      if (cp.base_tile == *it) {
        it++;
        continue;
      }
    }
    it = owned_base_tiles_.erase(it);
  }

  schema_ = std::move(new_schema);
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
inline int LogicalTile::AddPositionList(std::vector<oid_t> &&position_list) {
  assert(
      position_lists_.size() == 0
          || position_lists_[0].size() == position_list.size());

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
inline void LogicalTile::RemoveVisibility(oid_t tuple_id) {
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
inline storage::Tile *LogicalTile::GetBaseTile(oid_t column_id) {
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
// TODO: Deprecated. Avoid calling this function if possible.
inline Value LogicalTile::GetValue(oid_t tuple_id, oid_t column_id) {
  assert(column_id < schema_.size());
  assert(tuple_id < total_tuples_);
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
inline size_t LogicalTile::GetTupleCount() {
  return visible_tuples_;
}

/**
 * @brief Returns the number of columns.
 *
 * @return Number of columns.
 */
inline size_t LogicalTile::GetColumnCount() {
  return schema_.size();
}

/**
 * @brief Returns iterator pointing to first tuple.
 *
 * @return iterator pointing to first tuple.
 */
inline LogicalTile::iterator LogicalTile::begin() {
  bool begin = true;
  return iterator(this, begin);
}

/**
 * @brief Returns iterator indicating that we are past the last tuple.
 *
 * @return iterator indicating we're past the last tuple.
 */
inline LogicalTile::iterator LogicalTile::end() {
  bool begin = false;
  return iterator(this, begin);
}

/**
 * @brief Constructor for iterator.
 * @param Logical tile corresponding to this iterator.
 * @param begin Specifies whether we want the iterator initialized to point
 *              to the first tuple id, or to past-the-last tuple.
 */
inline LogicalTile::iterator::iterator(LogicalTile *tile, bool begin)
    : tile_(tile) {
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
inline LogicalTile::iterator &LogicalTile::iterator::operator++() {
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
inline LogicalTile::iterator LogicalTile::iterator::operator++(int) {
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
inline bool LogicalTile::iterator::operator==(const iterator &rhs) {
  return pos_ == rhs.pos_ && tile_ == rhs.tile_;
}

/**
 * @brief Inequality operator.
 * @param rhs The iterator to compare to.
 *
 * @return False if equal, true otherwise.
 */
inline bool LogicalTile::iterator::operator!=(const iterator &rhs) {
  return pos_ != rhs.pos_ || tile_ != rhs.tile_;
}

/**
 * @brief Dereference operator.
 *
 * @return Id of tuple that iterator is pointing at.
 */
inline oid_t LogicalTile::iterator::operator*() {
  return pos_;
}

}  // namespace executor
}  // namespace peloton
