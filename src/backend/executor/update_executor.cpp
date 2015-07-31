/*-------------------------------------------------------------------------
 *
 * update_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/executor/update_executor.h"

#include "backend/common/logger.h"
#include "backend/catalog/manager.h"
#include "backend/executor/logical_tile.h"
#include "backend/expression/container_tuple.h"
#include "backend/planner/update_node.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for update executor.
 * @param node Update node corresponding to this executor.
 */
UpdateExecutor::UpdateExecutor(planner::AbstractPlanNode *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DInit() {
  assert(children_.size() == 1);
  assert(target_table_ == nullptr);
  assert(project_info_ == nullptr);

  // Grab settings from node
  const planner::UpdateNode &node = GetPlanNode<planner::UpdateNode>();
  target_table_ = node.GetTable();
  project_info_ = node.GetProjectInfo();

  assert(target_table_);
  assert(project_info_);

  return true;
}

/**
 * @brief updates a set of columns
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DExecute() {
  assert(children_.size() == 1);
  assert(executor_context_);

  // We are scanning over a logical tile.
  LOG_TRACE("Update executor :: 1 child \n");

  if (!children_[0]->Execute()) {
    return false;
  }

  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  auto &pos_lists = source_tile.get()->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  auto transaction_ = executor_context_->GetTransaction();

  auto tile_group_id = tile_group->GetTileGroupId();
  auto &manager = catalog::Manager::GetInstance();

  // Update tuples in given table
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];
    LOG_TRACE("Visible Tuple id : %d, Physical Tuple id : %d \n",
              visible_tuple_id, physical_tuple_id);

    // (A) try to delete the tuple first
    // this might fail due to a concurrent operation that has latched the tuple
    auto delete_location = ItemPointer(tile_group_id, physical_tuple_id);
    bool status = target_table_->DeleteTuple(transaction_, delete_location);
    if (status == false) {
      transaction_->SetResult(Result::RESULT_FAILURE);
      return false;
    }
    transaction_->RecordDelete(delete_location);

    // (B.1) now, make a copy of the original tuple and allocate a new tuple
    expression::ContainerTuple<storage::TileGroup> old_tuple(tile_group, physical_tuple_id);
    storage::Tuple *new_tuple = new storage::Tuple(target_table_->GetSchema(), true);

    // (B.2) and execute the projections
    project_info_->Evaluate(new_tuple, &old_tuple, nullptr, executor_context_);

    // and finally insert into the table in update mode
    ItemPointer location =
        target_table_->UpdateTuple(transaction_, new_tuple);
    if (location.block == INVALID_OID) {
      new_tuple->FreeUninlinedData();
      delete new_tuple;
      return false;
    }
    transaction_->RecordInsert(location);
    new_tuple->FreeUninlinedData();
    delete new_tuple;

    // (C) set back pointer in tile group header of updated tuple
    auto updated_tile_group =
        static_cast<storage::TileGroup *>(manager.locator[location.block]);
    auto updated_tile_group_header = updated_tile_group->GetHeader();
    updated_tile_group_header->SetPrevItemPointer(
        location.offset, ItemPointer(tile_group_id, physical_tuple_id));
  }

  // By default, update should return nothing?
  // SetOutput(source_tile.release());
  return true;
}

}  // namespace executor
}  // namespace peloton
