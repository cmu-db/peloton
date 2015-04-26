/**
 * @brief Header file for utility functions for executor tests.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

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

  static executor::LogicalTile *ExecuteTile(
      executor::AbstractExecutor *executor,
      executor::LogicalTile *source_tile);
};

} // namespace test
} // namespace nstore
