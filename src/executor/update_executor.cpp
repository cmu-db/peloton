//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_executor.cpp
//
// Identification: src/executor/update_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/update_executor.h"
#include "planner/update_plan.h"
#include "common/logger.h"
#include "catalog/manager.h"
#include "executor/logical_tile.h"
#include "executor/executor_context.h"
#include "common/container_tuple.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"
#include "storage/tile_group_header.h"
#include "storage/tile.h"

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
  PL_ASSERT(children_.size() == 1);
  PL_ASSERT(target_table_ == nullptr);
  PL_ASSERT(project_info_ == nullptr);

  // Grab settings from node
  const planner::UpdatePlan &node = GetPlanNode<planner::UpdatePlan>();
  target_table_ = node.GetTable();
  project_info_ = node.GetProjectInfo();

  PL_ASSERT(target_table_);
  PL_ASSERT(project_info_);

  return true;
}

bool UpdateExecutor::PerformUpdatePrimaryKey(bool is_owner, oid_t tile_group_id,
                                             oid_t physical_tuple_id,
                                             ItemPointer &old_location,
                                             storage::TileGroup *tile_group) {

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  ///////////////////////////////////////
  // Delete tuple/version chain
  ///////////////////////////////////////
  ItemPointer new_location = target_table_->InsertEmptyVersion();

  // PerformUpdate() will not be executed if the insertion failed.
  // There is a write lock acquired, but since it is not in the write
  // set,
  // because we haven't yet put them into the write set.
  // the acquired lock can't be released when the txn is aborted.
  // the YieldOwnership() function helps us release the acquired write
  // lock.
  if (new_location.IsNull() == true) {
    LOG_TRACE("Fail to insert new tuple. Set txn failure.");
    if (is_owner == false) {
      // If the ownership is acquire inside this update executor, we
      // release it here
      transaction_manager.YieldOwnership(current_txn, tile_group_id,
                                         physical_tuple_id);
    }
    transaction_manager.SetTransactionResult(current_txn,
                                             ResultType::FAILURE);
    return false;
  }
  transaction_manager.PerformDelete(current_txn, old_location, new_location);

  ////////////////////////////////////////////
  // Insert tuple rather than install version
  ////////////////////////////////////////////
  auto target_table_schema = target_table_->GetSchema();

  storage::Tuple new_tuple(target_table_schema, true);

  expression::ContainerTuple<storage::TileGroup> old_tuple(tile_group,
                                                           physical_tuple_id);

  project_info_->Evaluate(&new_tuple, &old_tuple, nullptr, executor_context_);

  // insert tuple into the table.
  ItemPointer *index_entry_ptr = nullptr;
  peloton::ItemPointer location =
      target_table_->InsertTuple(&new_tuple, current_txn, &index_entry_ptr);

  // it is possible that some concurrent transactions have inserted the
  // same
  // tuple.
  // in this case, abort the transaction.
  if (location.block == INVALID_OID) {
    transaction_manager.SetTransactionResult(current_txn,
                                             peloton::ResultType::FAILURE);
    return false;
  }

  transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);

  return true;
}
/**
 * @brief updates a set of columns
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DExecute() {
  PL_ASSERT(children_.size() == 1);
  PL_ASSERT(executor_context_);

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

  auto current_txn = executor_context_->GetTransaction();

  commands::TriggerList* trigger_list = target_table_->GetTriggerList();
  if (trigger_list != nullptr) {
    LOG_TRACE("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
    if (trigger_list->HasTriggerType(commands::EnumTriggerType::BEFORE_UPDATE_STATEMENT)) {
      LOG_TRACE("target table has per-statement-before-update triggers!");
      trigger_list->ExecBSUpdateTriggers();
    }
  }

  // Update tuples in a given table
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    ItemPointer old_location(tile_group_id, physical_tuple_id);

    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ",
              visible_tuple_id, physical_tuple_id);

    if (trigger_list != nullptr) {
      LOG_TRACE("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
      if (trigger_list->HasTriggerType(commands::EnumTriggerType::BEFORE_UPDATE_ROW)) {
        LOG_TRACE("target table has per-row-before-update triggers!");
        trigger_list->ExecBRUpdateTriggers();
      }
    }

    bool is_owner = transaction_manager.IsOwner(current_txn, tile_group_header,
                                                physical_tuple_id);

    bool is_written = transaction_manager.IsWritten(
        current_txn, tile_group_header, physical_tuple_id);

    PL_ASSERT((is_owner == false && is_written == true) == false);

    // Prepare to examine primary key
    bool ret = false;
    const planner::UpdatePlan &update_node = GetPlanNode<planner::UpdatePlan>();

    if (is_owner == true && is_written == true) {
      if (update_node.GetUpdatePrimaryKey()) {
        // Update primary key
        ret =
            PerformUpdatePrimaryKey(is_owner, tile_group_id, physical_tuple_id,
                                    old_location, tile_group);

        // Examine the result
        if (ret == true) {
          executor_context_->num_processed += 1;  // updated one
        }
        // When fail, ownership release is done inside PerformUpdatePrimaryKey
        else {
          return false;
        }
      }

      // Normal update (no primary key)
      else {
        // We have already owned a version

        // Make a copy of the original tuple and allocate a new tuple
        expression::ContainerTuple<storage::TileGroup> old_tuple(
            tile_group, physical_tuple_id);
        // Execute the projections
        project_info_->Evaluate(&old_tuple, &old_tuple, nullptr,
                                executor_context_);

        transaction_manager.PerformUpdate(current_txn, old_location);
      }

      if (trigger_list != nullptr) {
        LOG_TRACE("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
        if (trigger_list->HasTriggerType(commands::EnumTriggerType::AFTER_UPDATE_ROW)) {
          LOG_TRACE("target table has per-row-after-update triggers!");
          trigger_list->ExecARUpdateTriggers();
        }
      }
    }
    // if we have already got the
    // ownership
    else {
      // Skip the IsOwnable and AcquireOwnership if we have already got the
      // ownership
      bool is_ownable =
          is_owner || transaction_manager.IsOwnable(
                          current_txn, tile_group_header, physical_tuple_id);

      if (is_ownable == true) {
        // if the tuple is not owned by any transaction and is visible to
        // current transaction.

        bool acquire_ownership_success =
            is_owner || transaction_manager.AcquireOwnership(
                            current_txn, tile_group_header, physical_tuple_id);

        if (acquire_ownership_success == false) {
          LOG_TRACE("Fail to insert new tuple. Set txn failure.");
          transaction_manager.SetTransactionResult(current_txn,
                                                   ResultType::FAILURE);
          return false;
        }

        if (update_node.GetUpdatePrimaryKey()) {
          // Update primary key
          ret = PerformUpdatePrimaryKey(is_owner, tile_group_id,
                                        physical_tuple_id, old_location,
                                        tile_group);

          if (ret == true) {
            executor_context_->num_processed += 1;  // updated one
          }
          // When fail, ownership release is done inside PerformUpdatePrimaryKey
          else {
            return false;
          }
        }

        // Normal update (no primary key)
        else {
          // if it is the latest version and not locked by other threads, then
          // insert a new version.

          // acquire a version slot from the table.
          ItemPointer new_location = target_table_->AcquireVersion();

          auto &manager = catalog::Manager::GetInstance();
          auto new_tile_group = manager.GetTileGroup(new_location.block);

          expression::ContainerTuple<storage::TileGroup> new_tuple(
              new_tile_group.get(), new_location.offset);

          expression::ContainerTuple<storage::TileGroup> old_tuple(
              tile_group, physical_tuple_id);

          // perform projection from old version to new version.
          // this triggers in-place update, and we do not need to allocate
          // another
          // version.
          project_info_->Evaluate(&new_tuple, &old_tuple, nullptr,
                                  executor_context_);

          // get indirection.
          ItemPointer *indirection =
              tile_group_header->GetIndirection(old_location.offset);
          // finally install new version into the table
          ret = target_table_->InstallVersion(&new_tuple,
                                              &(project_info_->GetTargetList()),
                                              current_txn, indirection);

          // PerformUpdate() will not be executed if the insertion failed.
          // There is a write lock acquired, but since it is not in the write
          // set,
          // because we haven't yet put them into the write set.
          // the acquired lock can't be released when the txn is aborted.
          // the YieldOwnership() function helps us release the acquired write
          // lock.
          if (ret == false) {
            LOG_TRACE("Fail to insert new tuple. Set txn failure.");
            if (is_owner == false) {
              // If the ownership is acquire inside this update executor, we
              // release it here
              transaction_manager.YieldOwnership(current_txn, tile_group_id,
                                                 physical_tuple_id);
            }
            transaction_manager.SetTransactionResult(current_txn,
                                                     ResultType::FAILURE);
            return false;
          }

          LOG_TRACE("perform update old location: %u, %u", old_location.block,
                    old_location.offset);
          LOG_TRACE("perform update new location: %u, %u", new_location.block,
                    new_location.offset);
          transaction_manager.PerformUpdate(current_txn, old_location,
                                            new_location);

          // TODO: Why don't we also do this in the if branch above?
          executor_context_->num_processed += 1;  // updated one
        }

        if (trigger_list != nullptr) {
          LOG_TRACE("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
          if (trigger_list->HasTriggerType(commands::EnumTriggerType::AFTER_UPDATE_ROW)) {
            LOG_TRACE("target table has per-row-after-update triggers!");
            trigger_list->ExecARUpdateTriggers();
          }
        }
      } else {

        // transaction should be aborted as we cannot update the latest version.
        LOG_TRACE("Fail to update tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(current_txn,
                                                 ResultType::FAILURE);
        return false;
      }
    }
  }

  if (trigger_list != nullptr) {
    LOG_TRACE("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
    if (trigger_list->HasTriggerType(commands::EnumTriggerType::AFTER_UPDATE_STATEMENT)) {
      LOG_TRACE("target table has per-statement-after-update triggers!");
      trigger_list->ExecASUpdateTriggers();
    }
  }
  return true;
}

}  // namespace executor
}  // namespace peloton
