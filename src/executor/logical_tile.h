/**
 * @brief Header for logical tile.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <iterator>
#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "common/value.h"

namespace nstore {

namespace storage {
class Tile;
class Tuple;
}

namespace executor {

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
      id_t origin_column_id,
      unsigned int position_list_idx);

  int AddPositionList(std::vector<id_t> &&position_list);

  void InvalidateTuple(id_t tuple_id);

  storage::Tile *GetBaseTile(id_t column_id);

  storage::Tuple *GetTuple(id_t column_id, id_t tuple_id);

  Value GetValue(id_t tuple_id, id_t column_id);

  int NumTuples();

  int NumCols();

  /**
   * @brief Iterates through tuple ids in this logical tile.
   *
   * This provides easy access to tuples that have not been invalidated.
   */
  class iterator : public std::iterator<std::input_iterator_tag, id_t> {
    // It's a friend so it can call this iterator's private constructor.
    friend class LogicalTile;

   public:
    iterator& operator++();

    iterator operator++(int);

    bool operator==(const iterator &rhs);

    bool operator!=(const iterator &rhs);

    id_t operator*();

   private:
    iterator(LogicalTile *tile, bool begin);

    /** @brief Keeps track of position of iterator. */
    id_t pos_;

    /** @brief Tile that this iterator is iterating over. */
    LogicalTile *tile_;
  };

  iterator begin();

  iterator end();

  friend std::ostream& operator<<(
      std::ostream& os, const LogicalTile& logical_tile);

 private:
  LogicalTile();

  /**
   * @brief An entry in a schema that links a position list to a corresponding
   *        column.
   */
  struct ColumnPointer {
    /** @brief Index of position list corresponding to this column. */
    unsigned int position_list_idx;

    /**
     * @brief Pointer to base tile that column is from.
     *
     * We use a pointer instead of the oid of the tile to minimize indirection.
     */
    storage::Tile *base_tile;

    /** @brief Original column id of this column in the base tile. */
    id_t origin_column_id;
  };

  /**
   * @brief Mapping of column ids in this logical tile to the underlying
   *        position lists and columns in base tiles.
   */
  std::vector<ColumnPointer> schema_;

  /**
   * @brief Lists of position lists.
   *
   * Each list contains positions corresponding to particular tiles/columns.
   */
  std::vector<std::vector<id_t> > position_lists_;

  /**
   * @brief Bit-vector storing validity of each row in the position lists.
   *
   * Used to cheaply invalidate rows of positions.
   */
  std::vector<bool> valid_rows_;

  /** @brief Keeps track of the number of tuples that are still valid. */
  int num_tuples_;

  /** @brief Set of base tiles owned (memory-wise) by this logical tile. */
  std::unordered_set<storage::Tile *> owned_base_tiles_;
};


} // namespace executor
} // namespace nstore

