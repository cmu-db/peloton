/**
 * @brief Header for logical tile.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <iterator>
#include <unordered_set>
#include <vector>

#include "catalog/schema.h"
#include "common/types.h"
#include "common/value.h"

namespace nstore {

namespace storage {
class Tile;
class Tuple;
}

namespace executor {

/*
 * Position list can be shared by multiple columns in a logical tile.
 * This reduces deduplication.
 */

class LogicalTile {
  friend class LogicalTileFactory;

 public:
  LogicalTile(const LogicalTile &) = delete;
  LogicalTile& operator=(const LogicalTile &) = delete;
  LogicalTile(LogicalTile &&) = delete;
  LogicalTile& operator=(LogicalTile &&) = delete;

  ~LogicalTile();

  void AddColumn(
      storage::Tile *base_tile,
      bool own_base_tile,
      oid_t origin_column_id,
      oid_t position_list_idx);

  int AddPositionList(std::vector<oid_t> &&position_list);

  void InvalidateTuple(oid_t tuple_id);

  storage::Tile *GetBaseTile(oid_t column_id);

  Value GetValue(oid_t tuple_id, oid_t column_id);

  size_t GetTupleCount();

  size_t GetColumnCount();

  catalog::Schema *GetSchema();

  bool IsWrapper();

  size_t GetWrappedTileCount();

  storage::Tile *GetWrappedTile(oid_t physical_tile_offset);

  /**
   * @brief Iterates through tuple ids in this logical tile.
   *
   * This provides easy access to tuples that have not been invalidated.
   */
  class iterator : public std::iterator<std::input_iterator_tag, oid_t> {
    // It's a friend so it can call this iterator's private constructor.
    friend class LogicalTile;

   public:
    iterator& operator++();

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

  friend std::ostream& operator<<(std::ostream& os, const LogicalTile& logical_tile);

  private:

  /** @brief Column metadata for logical tile */
  struct ColumnInfo {
    /** @brief Position list in logical tile that will correspond to this column. */
    oid_t position_list_idx;

    /**
     * @brief Pointer to base tile that column is from.
     * IMPORTANT: We use a pointer instead of the oid of the tile to minimize indirection.
     */
    storage::Tile *base_tile;

    /** @brief Original column id of this logical tile column in its associated base tile. */
    oid_t origin_column_id;
  };

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
  std::vector<std::vector<oid_t> > position_lists_;

  /**
   * @brief Bit-vector storing validity of each row in the position lists.
   * Used to cheaply invalidate rows of positions.
   */
  std::vector<bool> valid_rows_;

  /** @brief Keeps track of the number of tuples that are still valid. */
  oid_t num_tuples_ = 0;

  /** @brief Set of base tiles owned (memory-wise) by this logical tile. */
  std::unordered_set<storage::Tile *> owned_base_tiles_;

  /** @brief Is this logical tile just a wrapper around physical tiles ? */
  bool wrapper = false;

  /** @brief Set of base tiles wrapped by this logical tile. */
  std::vector<storage::Tile *> base_tiles_;
};


} // namespace executor
} // namespace nstore

