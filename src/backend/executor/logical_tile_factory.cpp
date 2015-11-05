//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logical_tile_factory.cpp
//
// Identification: src/backend/executor/logical_tile_factory.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/logical_tile_factory.h"

#include <memory>
#include <utility>

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace executor {

namespace {

/**
 * @brief Creates position list with the identity mapping.
 * @param size Size of position list.
 *
 * @return Position list.
 */
std::vector<oid_t> CreateIdentityPositionList(unsigned int size) {
  std::vector<oid_t> position_list(size);
  for (oid_t id = 0; id < size; id++) {
    position_list[id] = id;
  }
  return position_list;
}

}  // namespace

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
 *
 * @return Pointer to newly created logical tile.
 */
LogicalTile *LogicalTileFactory::WrapTiles(const std::vector<storage::Tile *> &base_tiles) {
  assert(base_tiles.size() > 0);

  // TODO ASSERT all base tiles have the same height.
  std::unique_ptr<LogicalTile> new_tile(new LogicalTile());

  // First, we build a position list to be shared by all the tiles.
  const oid_t position_list_idx = 0;
  new_tile->AddPositionList(
      CreateIdentityPositionList(base_tiles[0]->GetAllocatedTupleCount()));

  for (unsigned int i = 0; i < base_tiles.size(); i++) {
    // Next, we construct the schema.
    int column_count = base_tiles[i]->GetColumnCount();
    for (int col_id = 0; col_id < column_count; col_id++) {
      new_tile->AddColumn(base_tiles[i], col_id,
                          position_list_idx);
    }
  }

  // Drop reference because we created the base tile
  for(auto base_tile : base_tiles)
    base_tile->DecrementRefCount();

  return new_tile.release();
}

/**
 * @brief Convenience method to construct a logical tile wrapping a tile group.
 * @param tile_group Tile group to be wrapped.
 *
 * @return Logical tile wrapping tile group.
 */
LogicalTile *LogicalTileFactory::WrapTileGroup(storage::TileGroup *tile_group) {
  std::unique_ptr<LogicalTile> new_tile(new LogicalTile());

  const int position_list_idx = 0;
  new_tile->AddPositionList(CreateIdentityPositionList(tile_group->GetAllocatedTupleCount()));

  // Construct schema.
  std::vector<catalog::Schema> &schemas = tile_group->GetTileSchemas();
  assert(schemas.size() == tile_group->NumTiles());
  for (unsigned int i = 0; i < schemas.size(); i++) {
    storage::Tile *base_tile = tile_group->GetTile(i);
    for (oid_t col_id = 0; col_id < schemas[i].GetColumnCount(); col_id++) {
      new_tile->AddColumn(base_tile, col_id, position_list_idx);
    }
  }

  return new_tile.release();
}

/**
 * @brief Convenience method to construct a set of logical tiles wrapping a
 * given set of tuple locations potentially in multiple tile groups.
 * @param tuple_locations Tuple locations which are item pointers.
 *
 * @return Logical tile(s) wrapping the give tuple locations.
 */
std::vector<LogicalTile *> LogicalTileFactory::WrapTileGroups(
    const std::vector<ItemPointer> tuple_locations,
    const std::vector<oid_t> column_ids, txn_id_t txn_id, cid_t commit_id) {
  std::vector<LogicalTile *> result;

  // Get the list of blocks
  std::map<oid_t, std::vector<oid_t>> blocks;

  for (auto tuple_location : tuple_locations) {
    blocks[tuple_location.block].push_back(tuple_location.offset);
  }

  // Construct a logical tile for each block
  for (auto block : blocks) {
    LogicalTile *logical_tile = LogicalTileFactory::GetTile();

    auto &manager = catalog::Manager::GetInstance();
    storage::TileGroup *tile_group = manager.GetTileGroup(block.first);
    storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
    tile_group_header->IncrementRefCount();

    // Add relevant columns to logical tile
    logical_tile->AddColumns(tile_group, column_ids);

    // Print tile group visibility
    //tile_group_header->PrintVisibility(txn_id, commit_id);

    // Add visible tuples to logical tile
    std::vector<oid_t> position_list;
    for (auto tuple_id : block.second) {
      if (tile_group_header->IsVisible(tuple_id, txn_id, commit_id)) {
        position_list.push_back(tuple_id);
      }
    }

    tile_group_header->DecrementRefCount();
    logical_tile->AddPositionList(std::move(position_list));

    result.push_back(logical_tile);
  }

  return result;
}

}  // namespace executor
}  // namespace peloton
