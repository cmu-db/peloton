/**
 * @brief Implementation of logical tile factory.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/logical_tile_factory.h"

#include <memory>
#include <utility>

#include "common/types.h"
#include "executor/logical_tile.h"
#include "storage/tile.h"

namespace nstore {
namespace executor {

namespace {
  //TODO Implement function to verify that all base tiles in vector have the
  // same height.
}

/**
 * @brief Returns an empty logical tile.
 *
 * @return Pointer to empty logical tile.
 */
LogicalTile *LogicalTileFactory::GetTile() {
  return new LogicalTile();
}

/**
 * @brief Convenience method to construct a logical tile wrapping base tiles.
 * @param base_tiles Base tiles to be represented as a logical tile.
 * @param own_base_tiles True if the logical tile should own the base tiles.
 *
 * @return Pointer to newly created logical tile.
 */
LogicalTile *LogicalTileFactory::WrapBaseTiles(
    const std::vector<storage::Tile *> &base_tiles,
    bool own_base_tiles) {
  assert(base_tiles.size() > 0);
  //TODO ASSERT all base tiles have the same height.
  std::unique_ptr<LogicalTile> new_tile(new LogicalTile());

  // First, we build a position list to be shared by all the tiles.
  //TODO Modify logical tile to be able to represent lazily position lists that
  // span the entire tile.
  const id_t position_list_idx = 0;
  //TODO This should be active tuple count. But how to set it? High watermark?
  std::vector<id_t> position_list(base_tiles[0]->GetAllocatedTupleCount());
  for (id_t id = 0; id < position_list.size(); id++) {
    position_list[id] = id;
  }
  new_tile->AddPositionList(std::move(position_list));

  for (unsigned int i = 0; i < base_tiles.size(); i++) {
    // Next, we construct the schema.
    int column_count = base_tiles[i]->GetColumnCount();
    for (int col_id = 0; col_id < column_count; col_id++) {
      new_tile->AddColumn(
          base_tiles[i],
          own_base_tiles,
          col_id,
          position_list_idx);
    }
  }

  return new_tile.release();
}

} // namespace executor
} // namespace nstore
