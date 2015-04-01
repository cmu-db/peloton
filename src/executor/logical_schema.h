/**
 * @brief Header for logical schema.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <bitset>
#include <iostream>
#include <vector>

#include "common/types.h"

namespace nstore {

namespace storage {
  class Tile;
}

namespace executor {

class LogicalSchema {
 public:
  storage::Tile *GetBaseTile(id_t column_id);

  id_t GetOriginColumnId(id_t column_id);

  void AddColumn(storage::Tile *base_tile, id_t column_id);

  bool IsValid(id_t column_id);

  size_t NumCols();

  size_t NumValidCols();

  void InvalidateColumn(id_t column_id); 

  friend std::ostream& operator<<(
      std::ostream& os, const LogicalSchema& logical_schema);

 private:
  /** @brief Pointers to tiles that columns are from. */
  std::vector<storage::Tile *> base_tiles_;

  /** @brief Original column ids in base tiles of columns. */
  std::vector<id_t> origin_columns_;

  /**
   * @brief Valid bits of columns.
   *
   * Used to implement late-materialization for projection.
   * We don't use std::bitset because it requires the size at compile-time.
   */
  std::vector<bool> valid_bits_;
};

} // namespace executor
} // namespace nstore
