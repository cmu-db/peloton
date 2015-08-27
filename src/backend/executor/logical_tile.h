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

  void AddColumn(const ColumnInfo &cp, bool own_base_tile);

  void AddColumn(storage::Tile *base_tile, bool own_base_tile,
                 oid_t origin_column_id, oid_t position_list_idx);

  void AddColumns(storage::TileGroup *tile_group, const std::vector<oid_t> &column_ids);

  void ProjectColumns(const std::vector<oid_t> &original_column_ids, const std::vector<oid_t> &column_ids);

  int AddPositionList(std::vector<oid_t> &&position_list);

  void RemoveVisibility(oid_t tuple_id);

  storage::Tile *GetBaseTile(oid_t column_id);

  Value GetValue(oid_t tuple_id, oid_t column_id);

  size_t GetTupleCount();

  size_t GetColumnCount();

  const std::vector<ColumnInfo> &GetSchema() const;

  catalog::Schema *GetPhysicalSchema() const;

  void SetSchema(std::vector<LogicalTile::ColumnInfo> &&schema);

  const std::vector<std::vector<oid_t>> &GetPositionLists() const;

  void SetPositionLists(std::vector<std::vector<oid_t>> &&position_lists);

  void SetPositionListsAndVisibility(
      std::vector<std::vector<oid_t>> &&position_lists);

  void TransferOwnershipTo(LogicalTile *other);

  std::unordered_set<storage::Tile *> &GetOwnedBaseTiles();

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
    iterator &operator++();

    iterator operator++(int);

    bool operator==(const iterator &rhs);

    bool operator!=(const iterator &rhs);

    oid_t operator*();

   private:
    iterator(LogicalTile *tile, bool begin);

    /** @brief Keeps track of position of iterator. */
    oid_t pos_;

    /** @brief Tile that this iterator is iterating over. */
    LogicalTile *tile_;
  };

  iterator begin();

  iterator end();

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

  /** @brief Keeps track of the number of tuples that are still visible. */
  oid_t num_tuples_ = 0;

  /** @brief Set of base tiles owned (memory-wise) by this logical tile. */
  std::unordered_set<storage::Tile *> owned_base_tiles_;
};

}  // namespace executor
}  // namespace peloton
