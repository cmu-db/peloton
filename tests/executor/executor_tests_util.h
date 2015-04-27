/**
 * @brief Header file for utility functions for executor tests.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "common/types.h"

namespace nstore {

namespace catalog {
class Manager;
}

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace storage {
class Backend;
class TileGroup;
}

namespace test {

class ExecutorTestsUtil {
 public:
  static storage::TileGroup *CreateSimpleTileGroup(
      storage::Backend *backend,
      int tuple_count);

  static void PopulateTiles(storage::TileGroup *tile_group, int num_rows);

  static executor::LogicalTile *ExecuteTile(executor::AbstractExecutor *executor,
                          executor::LogicalTile *source_logical_tile);

  /**
   * @brief Returns the value populated at the specified field.
   * @param tuple_id Row of the specified field.
   * @param column_id Column of the specified field.
   *
   * This method defines the values that are populated by PopulateTiles().
   *
   * @return Populated value.
   */
  inline static int PopulatedValue(const id_t tuple_id, const id_t column_id) {
    return 10 * tuple_id + column_id;
  }
};

} // namespace test
} // namespace nstore
