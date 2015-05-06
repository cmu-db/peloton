/**
 * @brief Executor for sequential scan node.
 *
 * Possible optimization: Right now we loop through the tile group in the
 * scan and apply the predicate tuple at a time. Instead, we might want to
 * refactor the expression system so we can apply predicates to fields in
 * different tiles separately, and then combine the results.
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/seq_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "expression/abstract_expression.h"
#include "expression/container_tuple.h"
#include "planner/seq_scan_node.h"
#include "storage/table.h"
#include "storage/tile_group.h"

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
  assert(children_.size() == 0 || children_.size() == 1);
  return true;
}

/**
 * @brief Creates logical tile from tile group and applys scan predicate.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DExecute() {
  assert(children_.size() == 0 || children_.size() == 1);

  // Grab data from plan node.
  const planner::SeqScanNode &node = GetNode<planner::SeqScanNode>();
  const storage::Table *table = node.table();
  const expression::AbstractExpression *predicate = node.predicate();
  const std::vector<id_t> &column_ids = node.column_ids();

  // We are scanning over a logical tile.
  if (children_.size() == 1) {
    if (!children_[0]->Execute()) {
      return false;
    }

    std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());
    if (predicate != nullptr) {
      // Invalidate tuples that don't satisfy the predicate.
      for (id_t tuple_id : *tile) {
        expression::ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
        if (predicate->Evaluate(&tuple, nullptr).IsFalse()) {
          tile->InvalidateTuple(tuple_id);
        }
      }
    }

    SetOutput(tile.release());
    return true;
  }

  // We are scanning a table (this is a leaf node in the plan tree).
  assert(children_.size() == 0);
  assert(table != nullptr);
  assert(column_ids.size() > 0);

  // Retrieve next tile group.
  if (current_tile_group_id_ == table->GetTileGroupCount()) {
    return false;
  }

  storage::TileGroup *tile_group =
    table->GetTileGroup(current_tile_group_id_++);

  // Construct position list by looping through tile group and
  // applying the predicate.
  std::vector<id_t> position_list;
  for (id_t tuple_id = 0;
      tuple_id < tile_group->GetActiveTupleCount();
      tuple_id++) {
    expression::ContainerTuple<storage::TileGroup> tuple(tile_group, tuple_id);
    if (predicate == nullptr
        || predicate->Evaluate(&tuple, nullptr).IsTrue()) {
      position_list.push_back(tuple_id);
    }
  }

  // Construct logical tile.
  std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
  const bool own_base_tile = false;
  const int position_list_idx = 0;
  logical_tile->AddPositionList(std::move(position_list));
  for (id_t origin_column_id : column_ids) {
    id_t base_tile_id, tile_column_id;
    tile_group->LocateTileAndColumn(
        origin_column_id,
        tile_column_id,
        base_tile_id);
    logical_tile->AddColumn(
        tile_group->GetTile(base_tile_id),
        own_base_tile,
        tile_column_id,
        position_list_idx);
  }

  SetOutput(logical_tile.release());
  return true;
}

} // namespace executor
} // namespace nstore
