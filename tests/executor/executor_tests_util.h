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
};

} // namespace test
} // namespace nstore
