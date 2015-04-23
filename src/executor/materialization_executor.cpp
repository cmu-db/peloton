/**
 * @brief Executor for materialization node.
 *
 * This executor also performs all functions of a projection node, in order
 * to support late materialization).
 *
 * TODO Integrate expression system into materialization.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/materialization_executor.h"

#include <cassert>

#include <memory>
#include <utility>

#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "planner/materialization_node.h"
#include "storage/tile.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for the materialization executor.
 * @param node Materialization node corresponding to this executor.
 */
MaterializationExecutor::MaterializationExecutor(
    const planner::AbstractPlanNode *node)
  : AbstractExecutor(node) {
}

/**
 * @brief Nothing to init at the moment.
 *
 * @return True on success, false otherwise.
 */
bool MaterializationExecutor::SubInit() {
  assert(children_.size() == 1);
  return true;
}

/**
 * @brief Creates materialized physical tile from logical tile and wraps it
 *        in a new logical tile.
 *
 * @return Pointer to logical tile containing newly materialized physical tile.
 */
LogicalTile *MaterializationExecutor::SubGetNextTile() {
  assert(children_.size() == 1);

  // Retrieve next tile.
  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetNextTile());
  if (source_tile.get() == nullptr) {
    return nullptr;
  }

  const planner::MaterializationNode &node =
    GetNode<planner::MaterializationNode>();
  const std::unordered_map<id_t, id_t> &old_to_new_cols =
    node.old_to_new_cols();

  // Generate mappings.
  std::unordered_map<storage::Tile *, std::vector<id_t> > tile_to_cols;
  GenerateTileToColMap(
      old_to_new_cols,
      source_tile.get(),
      tile_to_cols);

  // Create new physical tile.
  const int num_tuples = source_tile->NumTuples();
  std::unique_ptr<storage::Tile> dest_tile(
      storage::TileFactory::GetTempTile(
        node.schema(),
        num_tuples));

  // Proceed to materialize by tile.
  MaterializeByTiles(
      source_tile.get(),
      old_to_new_cols,
      tile_to_cols,
      dest_tile.get());

  // Wrap physical tile in logical tile.
  bool own_base_tile = true;
  return LogicalTileFactory::WrapBaseTiles(
      { dest_tile.release() },
      own_base_tile);
}

/**
 * @brief Generates map from each base tile to columns originally from that
 *        base tile to be materialized.
 * @param column_ids Ids of columns to be materialized.
 * @param source_tile Logical tile that contains mapping from columns to
 *        base tiles.
 * @param tile_to_cols Map to be populated with mappings from tile to columns.
 *
 * We generate this mapping so that we can materialize columns by tile for
 * efficiency reasons.
 */
void MaterializationExecutor::GenerateTileToColMap(
    const std::unordered_map<id_t, id_t> &old_to_new_cols,
    LogicalTile *source_tile,
    std::unordered_map<storage::Tile *, std::vector<id_t> > &tile_to_cols) {
  for (const auto &kv : old_to_new_cols) {
    id_t col = kv.first;
    storage::Tile *base_tile = source_tile->GetBaseTile(col);
    std::vector<id_t> &cols_from_tile = tile_to_cols[base_tile];
    cols_from_tile.push_back(col);
  }
}

/**
 * @brief Does the actual copying of data into the new tile.
 * @param source_tile Source tile to copy data from.
 * @param tile_to_cols Map from base tile to columns in that tile
 *        to be materialized.
 * @param dest_tile New tile to copy data into.
 */
void MaterializationExecutor::MaterializeByTiles(
    LogicalTile *source_tile,
    const std::unordered_map<id_t, id_t> &old_to_new_cols,
    const
    std::unordered_map<storage::Tile *, std::vector<id_t> > &tile_to_cols,
    storage::Tile *dest_tile) {
  for (const auto &kv : tile_to_cols) {
    // Copy over all data from each base tile.
    const std::vector<id_t> &old_column_ids = kv.second;
    for (id_t old_col_id : old_column_ids) {
      int new_tuple_id = 0;
      auto it = old_to_new_cols.find(old_col_id);
      assert(it != old_to_new_cols.end());
      id_t new_col_id = it->second;
      for (id_t old_tuple_id : *source_tile) {
        Value value = source_tile->GetValue(old_col_id, old_tuple_id);
        dest_tile->SetValue(value, new_tuple_id++, new_col_id);
      }
    }
  }
}

} // namespace executor
} // namespace nstore
