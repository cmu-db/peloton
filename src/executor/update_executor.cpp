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

bool UpdateExecutor::PerformUpdatePrimaryKey(bool is_owner,
                                             storage::TileGroup *tile_group, 
                                             storage::TileGroupHeader *tile_group_header,
                                             oid_t physical_tuple_id,
                                             ItemPointer &old_location) {

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
      transaction_manager.YieldOwnership(current_txn, tile_group_header,
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
  // same tuple. In this case, abort the transaction.
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
  
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  // Update tuples in a given table
  for (oid_t visible_tuple_id : *source_tile) {

    storage::TileGroup *tile_group = source_tile->GetBaseTile(0)->GetTileGroup();
    storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
    
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    ItemPointer old_location(tile_group->GetTileGroupId(), physical_tuple_id);

    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ",
              visible_tuple_id, physical_tuple_id);

    ///////////////////////////////////////////////////////////
    // if running at snapshot isolation, 
    // then we need to retrieve the latest version of this tuple.
    if (current_txn->GetIsolationLevel() == IsolationLevelType::SNAPSHOT) {
      old_location = *(tile_group_header->GetIndirection(physical_tuple_id));
      
      auto &manager = catalog::Manager::GetInstance();
      tile_group = manager.GetTileGroup(old_location.block).get();
      tile_group_header = tile_group->GetHeader();

      physical_tuple_id = old_location.offset;

      auto visibility = transaction_manager.IsVisible(
          current_txn, tile_group_header, physical_tuple_id, VisibilityIdType::COMMIT_ID);
      if (visibility != VisibilityType::OK) {
        transaction_manager.SetTransactionResult(current_txn,
                                                 ResultType::FAILURE);
        return false;
      }
    }
    ///////////////////////////////////////////////////////////

    bool is_owner = 
        transaction_manager.IsOwner(current_txn, tile_group_header, physical_tuple_id);

    bool is_written = 
        transaction_manager.IsWritten(current_txn, tile_group_header, physical_tuple_id);

    // Prepare to examine primary key
    bool ret = false;
    const planner::UpdatePlan &update_node = GetPlanNode<planner::UpdatePlan>();

    // if the current transaction is the creator of this version.
    // which means the current transaction has already updated the version.
    if (is_owner == true && is_written == true) {

      if (update_node.GetUpdatePrimaryKey()) {
        // Update primary key
        ret =
            PerformUpdatePrimaryKey(is_owner, tile_group, 
                                    tile_group_header, physical_tuple_id,
                                    old_location);

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
    }
    // if we have already obtained the ownership
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
          ret = PerformUpdatePrimaryKey(is_owner, tile_group, 
                                        tile_group_header, physical_tuple_id, 
                                        old_location);

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
          // another version.
          project_info_->Evaluate(&new_tuple, &old_tuple, nullptr,
                                  executor_context_);

          // get indirection.
          ItemPointer *indirection =
              tile_group_header->GetIndirection(old_location.offset);

          // Indicate later whether this is a foreign key constraint failure
          bool fk_failure;    

          // finally install new version into the table
          ret = target_table_->InstallVersion(&new_tuple,
                                              &(project_info_->GetTargetList()),
                                              current_txn, indirection,
                                              fk_failure);

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
              transaction_manager.YieldOwnership(current_txn, tile_group_header,
                                                 physical_tuple_id);
            }
            transaction_manager.SetTransactionResult(current_txn,
                                                     ResultType::FAILURE);
            if (fk_failure) {
              throw ConstraintException("FOREIGN KEY constraint violated : " +
                               std::string(new_tuple.GetInfo()));
            } else {
              throw ConstraintException("Secondary index constraint violated : " +
                               std::string(new_tuple.GetInfo()));
            }

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
      } else {

        // transaction should be aborted as we cannot update the latest version.
        LOG_TRACE("Fail to update tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(current_txn,
                                                 ResultType::FAILURE);
        return false;
      }
    }
  }
  return true;
}

}  // namespace executor
}  // namespace peloton
