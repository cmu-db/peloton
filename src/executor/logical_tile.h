/**
 * @brief Header for logical tile.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

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

 public:
  void AddColumn(
      storage::Tile *base_tile,
      id_t origin_column_id,
      unsigned int position_list_idx);

  int AddPositionList(std::vector<id_t> &&position_list);

  storage::Tile *GetBaseTile(id_t column_id);

  storage::Tuple *GetTuple(id_t column_id, id_t tuple_id);

  Value GetValue(id_t column_id, id_t tuple_id);

  friend std::ostream& operator<<(
      std::ostream& os, const LogicalTile& logical_tile);

 private:
  /**
   * @brief An entry in a schema that links a position list to a corresponding
   *        column.
   */
  struct ColumnPointer {
    /** @brief Index of position list corresponding to this column. */
    unsigned int position_list_idx;

    /** @brief Pointer to base tile that column is from. */
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
};


} // namespace executor
} // namespace nstore

