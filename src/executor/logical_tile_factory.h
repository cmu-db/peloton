/**
 * @brief Header for logical tile factory.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <vector>

namespace nstore {

namespace storage {
class Tile;
}

namespace executor {
class LogicalTile;

class LogicalTileFactory {
 public:
  static LogicalTile *GetTile();

  static LogicalTile *WrapBaseTiles(
      const std::vector<storage::Tile *> &base_tile,
      bool own_base_tiles);
};

} // namespace executor
} // namespace nstore
