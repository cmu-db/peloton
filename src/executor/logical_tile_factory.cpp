/**
 * @brief Implementation of logical tile factory.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/logical_tile_factory.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/types.h"
#include "executor/logical_tile.h"
#include "storage/tile.h"

namespace nstore {
namespace executor {

/**
 * @brief Returns an empty logical tile.
 *
 * @return Pointer to empty logical tile.
 */
LogicalTile *LogicalTileFactory::GetTile() {
  return new LogicalTile();
}

/**
 * @brief Convenience method to construct a logical tile wrapping a single
 *        base tile.
 * @param base_tile Base tile that is represented as a logical tile.
 * @param own_base_tile True if the logical tile should own the base tile.
 *
 * @return Pointer to newly created logical tile.
 */
LogicalTile *LogicalTileFactory::WrapBaseTile(
    storage::Tile *base_tile,
    bool own_base_tile) {
  std::unique_ptr<LogicalTile> new_tile(new LogicalTile());

  // First, we build a position list for the entire tile.
  //TODO Modify logical tile to be able to represent lazily position lists that
  // span the entire tile.
  id_t position_list_idx = 0;
  //TODO Do we want the allocated tuple count? Or just active tuples?
  std::vector<id_t> position_list(base_tile->GetAllocatedTupleCount());
  for (id_t id = 0; id < position_list.size(); id++) {
    position_list[id] = id;
  }
  new_tile->AddPositionList(std::move(position_list));

  // Next, we construct the schema.
  for (int col_id = 0; col_id < base_tile->GetColumnCount(); col_id++) {
    new_tile->AddColumn(
        base_tile,
        own_base_tile,
        col_id,
        position_list_idx);
  }

  return new_tile.release();
}

} // namespace executor
} // namespace nstore
