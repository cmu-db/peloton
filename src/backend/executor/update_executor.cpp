//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_executor.cpp
//
// Identification: src/backend/executor/update_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/update_executor.h"
#include "backend/planner/update_plan.h"
#include "backend/common/logger.h"
#include "backend/catalog/manager.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/container_tuple.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile.h"
#include "backend/storage/rollback_segment.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for update executor.
 * @param node Update node corresponding to this executor.
 */
UpdateExecutor::UpdateExecutor(const planner::AbstractPlan *node,
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
  const planner::UpdatePlan &node = GetPlanNode<planner::UpdatePlan>();
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
  LOG_TRACE("Update executor :: 1 child ");

  if (!children_[0]->Execute()) {
    return false;
  }

  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  auto &pos_lists = source_tile.get()->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
  auto tile_group_id = tile_group->GetTileGroupId();

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto concurrency_protocol = concurrency::TransactionManagerFactory::GetProtocol();
  auto schema = target_table_->GetSchema();

  // Update tuples in given table
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    ItemPointer old_location(tile_group_id, physical_tuple_id);


    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ",
              visible_tuple_id, physical_tuple_id);

    if (transaction_manager.IsOwner(tile_group_header, physical_tuple_id) == true) {

      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(
          tile_group, physical_tuple_id);
      // Create a temp copy
      std::unique_ptr<storage::Tuple> new_tuple(new storage::Tuple(target_table_->GetSchema(), true));
      // Execute the projections
      // FIXME: reduce memory copy by doing inplace update
      project_info_->Evaluate(new_tuple.get(), &old_tuple, nullptr,
                              executor_context_);

      // Check if we are using rollback segment
      if (concurrency_protocol == CONCURRENCY_TYPE_OCC_RB) {
        auto rb_txn_manager = (concurrency::OptimisticRbTxnManager*)&transaction_manager;

        if (rb_txn_manager->IsInserted(tile_group_header, physical_tuple_id) == false) {
          // If it's not an inserted tuple,
          // create a new rollback segment based on the old one and the old tuple
          auto rb_seg = rb_txn_manager->GetSegmentPool()->CreateSegmentFromTuple(
            schema, project_info_->GetTargetList(), &old_tuple);

          // TODO: rb_seg == nullptr may be resulted from an optimization to be done
          // when creating rollback segment
          if (rb_seg != nullptr) {
            // Ask the txn manager to add the a new rollback segment
            rb_txn_manager->PerformUpdateWithRb(old_location, rb_seg);
          }
        }

        // Overwrite the master copy
        tile_group->CopyTuple(new_tuple.get(), physical_tuple_id);

      } else {
        // Current rb segment is OK, just overwrite the tuple in place
        tile_group->CopyTuple(new_tuple.get(), physical_tuple_id);
        transaction_manager.PerformUpdate(old_location);
      }

    } else if (transaction_manager.IsOwnable(tile_group_header,
                                             physical_tuple_id) == true) {
      // if the tuple is not owned by any transaction and is visible to current
      // transaction.

      if (transaction_manager.AcquireOwnership(tile_group_header, tile_group_id,
                                               physical_tuple_id) == false) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        return false;
      }
      // if it is the latest version and not locked by other threads, then
      // insert a new version.
      std::unique_ptr<storage::Tuple> new_tuple(new storage::Tuple(target_table_->GetSchema(), true));

      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(
          tile_group, physical_tuple_id);
      // Execute the projections
      project_info_->Evaluate(new_tuple.get(), &old_tuple, nullptr,
                              executor_context_);

      if (concurrency_protocol == CONCURRENCY_TYPE_OCC_RB) {
        // For rollback segment implementation
        auto rb_txn_manager = (concurrency::OptimisticRbTxnManager*)&transaction_manager;

        // Create a rollback segment based on the old tuple
        auto rb_seg = rb_txn_manager->GetSegmentPool()->CreateSegmentFromTuple(
          schema, project_info_->GetTargetList(), &old_tuple);

        // Ask the txn manager to append the rollback segment
        rb_txn_manager->PerformUpdateWithRb(old_location, rb_seg);

        // Overwrite the master copy
        tile_group->CopyTuple(new_tuple.get(), old_location.offset);
      } else {
        // finally insert updated tuple into the table
        ItemPointer new_location = target_table_->InsertVersion(new_tuple.get());

        // FIXME: PerformUpdate() will not be executed if the insertion failed,
        // There is a write lock acquired, but it is not in the write set.
        // the acquired lock can't be released when the txn is aborted.
        if (new_location.IsNull() == true) {
          LOG_TRACE("Fail to insert new tuple. Set txn failure.");
          // First yield ownership
          transaction_manager.YieldOwnership(tile_group_id, physical_tuple_id);
          transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
          return false;
        }

        LOG_INFO("perform update old location: %u, %u", old_location.block, old_location.offset);
        LOG_INFO("perform update new location: %u, %u", new_location.block, new_location.offset);
        transaction_manager.PerformUpdate(old_location, new_location);
      }

      // TODO: Why don't we also do this in the if branch above?
      executor_context_->num_processed += 1;  // updated one
    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
      return false;
    }
  }
  return true;
}

}  // namespace executor
}  // namespace peloton
