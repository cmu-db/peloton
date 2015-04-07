/**
 * @brief Executor for materialization node.
 *
 * This executor also performs all functions of a projection node, in order
 * to support late materialization).
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/materialization_executor.h"

#include <cassert>

#include <memory>
#include <utility>
#include <vector>

#include "executor/logical_tile.h"
#include "planner/materialization_node.h"

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

  // Generate mappings.
  std::unordered_map<storage::Tile *, std::vector<id_t> > tile_to_cols;
  GenerateTileToColMap(source_tile.get(), tile_to_cols);

  // Proceed to materialize by tile.
  // Create storage::tile
  //TODO Iterate over base tiles and materialize.

  return nullptr;
}

/** @brief Nothing to clean up at the moment. */
void MaterializationExecutor::SubCleanUp() {}

/**
 * @brief Generates map from each base tile to columns originally from that
 *        base tile to be materialized.
 * 
 * We generate this mapping so that we can materialize columns by tile for
 * efficiency reasons.
 */
void MaterializationExecutor::GenerateTileToColMap(
    LogicalTile *source_tile,
    std::unordered_map<storage::Tile *, std::vector<id_t> > &tile_to_cols) {
  planner::MaterializationNode &node = GetNode<planner::MaterializationNode>();
  const std::vector<id_t> &column_ids = node.column_ids();

  for (unsigned int i = 0; i < column_ids.size(); i++) {
    id_t col = column_ids[i];
    storage::Tile *base_tile = source_tile->GetBaseTile(col);
    std::vector<id_t> &cols_from_tile = tile_to_cols[base_tile];
    cols_from_tile.push_back(col);
  }
}

} // namespace executor
} // namespace nstore
