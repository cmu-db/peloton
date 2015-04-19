/**
 * @brief Header file for utility functions for executor tests.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

namespace nstore {

namespace storage {
  class Tile;
}

namespace test {

class ExecutorTestsUtil {
 public:
  storage::Tile *CreateSimplePhysicalTile();
};

} // namespace test
} // namespace nstore
