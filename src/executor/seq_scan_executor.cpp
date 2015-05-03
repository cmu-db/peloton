/**
 * @brief Executor for sequential scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/seq_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "catalog/manager.h"
#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "expression/abstract_expression.h"
#include "planner/seq_scan_node.h"
#include "storage/table.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tile_iterator.h"
#include "storage/tuple.h"

namespace nstore {
namespace executor {

/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
SeqScanExecutor::SeqScanExecutor(planner::AbstractPlanNode *node)
  : AbstractExecutor(node) {
}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DInit() {
  assert(children_.size() == 0);
  return true;
}

/**
 * @brief Creates logical tile from tile group and applys scan predicate.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DExecute() {
  assert(children_.size() == 0);

  // Grab data from plan node.
  const planner::SeqScanNode &node = GetNode<planner::SeqScanNode>();
  const oid_t table_id = node.table_id();
  const expression::AbstractExpression *predicate = node.predicate();
  const std::vector<id_t> &column_ids = node.column_ids();

  auto &locator = catalog::Manager::GetInstance().locator;
  // TODO Do this in DInit() to amortize lookup over all tiles.
  // Do we have to do some kind of synchronization to ensure that the table
  // doesn't disappear midway through our request?
  auto it = locator.find(table_id);
  if (it == locator.end()) {
    return false;
  }

  // Retrieve next available physical (base) tile.
  // TODO Are we supposed to go through database or catalog::Table first?
  storage::Table *table = static_cast<storage::Table *>(it->second);
  //TODO Create iterators for table and tile groups?
  if (current_tile_group_id_ == table->GetTileGroupCount()) {
    return false;
  }
  storage::TileGroup *tile_group = table->GetTileGroup(current_tile_group_id_);
  if (current_tile_id_ == tile_group->NumTiles()) {
    current_tile_group_id_++;
    current_tile_id_ = 0;
    if (current_tile_group_id_ == table->GetTileGroupCount()) {
      return false;
    }
    tile_group = table->GetTileGroup(current_tile_group_id_);
    assert(tile_group->NumTiles() > 0);
  }
  storage::Tile *base_tile = tile_group->GetTile(current_tile_id_++);

  // Construct position list by looping through tile group and
  // applying the predicate.
  std::vector<id_t> position_list;
  storage::TileIterator tile_it(base_tile);
  storage::Tuple tuple(&base_tile->GetSchema());
  while (tile_it.Next(tuple)) {
    if (predicate == nullptr
        || predicate->Evaluate(&tuple, nullptr).IsTrue()) {
      // GetLocation() returns the id of the next tuple.
      assert(tile_it.GetLocation() > 0);
      position_list.push_back(tile_it.GetLocation() - 1);
    }
  }

  // Construct logical tile.
  std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
  const bool own_base_tile = false;
  const int position_list_idx = 0;
  logical_tile->AddPositionList(std::move(position_list));
  for (id_t origin_column_id : column_ids) {
    logical_tile->AddColumn(
        base_tile,
        own_base_tile,
        origin_column_id,
        position_list_idx);
  }

  SetOutput(logical_tile.release());
  return true;
}

} // namespace executor
} // namespace nstore
