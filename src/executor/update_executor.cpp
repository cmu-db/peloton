//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_executor.cpp
//
// Identification: src/executor/update_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/update_executor.h"
#include "planner/update_plan.h"
#include "common/logger.h"
#include "catalog/manager.h"
#include "executor/logical_tile.h"
#include "executor/executor_context.h"
#include "common/container_tuple.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"
#include "storage/tile_group_header.h"
#include "storage/tile.h"
#include "storage/storage_manager.h"
#include "catalog/foreign_key.h"

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

bool UpdateExecutor::PerformUpdatePrimaryKey(
    bool is_owner, storage::TileGroup *tile_group,
    storage::TileGroupHeader *tile_group_header, oid_t physical_tuple_id,
    ItemPointer &old_location) {
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  auto target_table_schema = target_table_->GetSchema();

  ContainerTuple<storage::TileGroup> old_tuple(tile_group, physical_tuple_id);
  storage::Tuple prev_tuple(target_table_schema, true);

  // Get a copy of the old tuple
  for (oid_t column_itr = 0; column_itr < target_table_schema->GetColumnCount(); column_itr++) {
    type::Value val = (old_tuple.GetValue(column_itr));
    prev_tuple.SetValue(column_itr, val, executor_context_->GetPool());
  }

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
    transaction_manager.SetTransactionResult(current_txn, ResultType::FAILURE);
    return false;
  }
  transaction_manager.PerformDelete(current_txn, old_location, new_location);

  ////////////////////////////////////////////
  // Insert tuple rather than install version
  ////////////////////////////////////////////
  storage::Tuple new_tuple(target_table_schema, true);

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

  // Check the source table of any foreign key constraint
  size_t fk_count = target_table_->GetForeignKeySrcCount();
  if (fk_count > 0) {
    for (size_t iter = 0; iter < fk_count; iter++) {
      catalog::ForeignKey *fk = target_table_->GetForeignKeySrc(iter);
      
      // Check if any row in the source table references the current tuple
      oid_t source_table_id = fk->GetSourceTableOid();
      storage::DataTable *src_table = nullptr;
      try {
        src_table = (storage::DataTable *)storage::StorageManager::GetInstance()
                        ->GetTableWithOid(target_table_->GetDatabaseOid(), source_table_id);
      } catch (CatalogException &e) {
        LOG_TRACE("Can't find table %d! Return false", source_table_id);
        transaction_manager.SetTransactionResult(current_txn,
                                                 peloton::ResultType::FAILURE);
        return false;
      }

      int src_table_index_count = src_table->GetIndexCount();
      for (int iter = 0; iter < src_table_index_count; iter++) {
        auto index = src_table->GetIndex(iter);
        if (index == nullptr) continue;

        // Make sure this is the right index to search in
        if (index->GetMetadata()->GetName().find("_FK_") != std::string::npos &&
            index->GetMetadata()->GetKeyAttrs() == fk->GetSourceColumnIds())
        {
          LOG_DEBUG("Searching in source tables's fk index...\n"); 

          std::vector<oid_t> key_attrs = fk->GetSourceColumnIds();
          std::unique_ptr<catalog::Schema> fk_schema(
            catalog::Schema::CopySchema(src_table->GetSchema(), key_attrs));
          std::unique_ptr<storage::Tuple> key(
            new storage::Tuple(fk_schema.get(), true));
          // std::unique_ptr<storage::Tuple> new_key(
          //   new storage::Tuple(fk_schema.get(), true));
          
          key->SetFromTuple(&prev_tuple, fk->GetSinkColumnIds(), index->GetPool());

          // new_key->SetFromTuple(&new_tuple, fk->GetSinkColumnIds(), index->GetPool());

          // is this necessary ? 
          // if ((*key) == (*new_key)) {
          //   LOG_DEBUG("Foreign key column(s) not changed\n");
          //   break;
          // }
          
          std::vector<ItemPointer *> location_ptrs;
          index->ScanKey(key.get(), location_ptrs);

          if (location_ptrs.size() > 0) {
            LOG_DEBUG("Something found in the source table!\n");

            for (ItemPointer *ptr : location_ptrs) {
              auto src_tile_group = src_table->GetTileGroupById(ptr->block);
              auto src_tile_group_header = src_tile_group->GetHeader();

              auto visibility = transaction_manager.IsVisible(
                current_txn, src_tile_group_header, ptr->offset,
                VisibilityIdType::COMMIT_ID);

              if (visibility != VisibilityType::OK) continue;

              switch (fk->GetUpdateAction()) {
                // Currently NOACTION is the same as RESTRICT
                case FKConstrActionType::NOACTION:
                case FKConstrActionType::RESTRICT: {
                  transaction_manager.SetTransactionResult(current_txn,
                                                          peloton::ResultType::FAILURE);
                  return false;
                }
                case FKConstrActionType::CASCADE: 
                default: {
                  // Update
                  bool src_is_owner = transaction_manager.IsOwner(current_txn,
                      src_tile_group_header, ptr->offset);

                  // if (src_is_owner == false) {
                  //   bool is_ownable = transaction_manager.IsOwnable(
                  //           current_txn, src_tile_group_header, ptr->offset);
                    
                  //   if (is_ownable) {
                  //     bool try_result = transaction_manager.AcquireOwnership(current_txn, src_tile_group_header,
                  //                                ptr->offset);
                  //     if (try_result == false) {
                  //       LOG_DEBUG("Failed to own the version in the fk src table\n");
                  //       transaction_manager.SetTransactionResult(current_txn,
                  //                                  ResultType::FAILURE);
                  //       return false;
                  //     }
                  //   } else {
                  //     LOG_DEBUG("Cannot own the version in the fk src table\n");
                  //     transaction_manager.SetTransactionResult(current_txn,
                  //                                         peloton::ResultType::FAILURE);
                  //     return false;
                  //   }
                  // }

                  // Read the referencing tuple, update the read timestamp so that we can
                  // delete it later
                  bool ret = transaction_manager.PerformRead(current_txn,
                                                             *ptr,
                                                             true);

                  if (ret == false) {
                    if (src_is_owner) {
                      transaction_manager.YieldOwnership(current_txn, src_tile_group_header,
                                                         ptr->offset);
                    }
                    transaction_manager.SetTransactionResult(current_txn,
                                                             peloton::ResultType::FAILURE);
                    return false;
                  }

                  ContainerTuple<storage::TileGroup> src_old_tuple(src_tile_group.get(), ptr->offset);
                  storage::Tuple src_new_tuple(src_table->GetSchema(), true);

                  for (oid_t col_itr = 0; col_itr < src_table->GetSchema()->GetColumnCount(); col_itr++)
                  {
                    type::Value val = src_old_tuple.GetValue(col_itr);
                    src_new_tuple.SetValue(col_itr, val, executor_context_->GetPool());
                  }

                  // Set the primary key fields
                  for (oid_t k = 0; k < key_attrs.size(); k++) {
                    auto src_col_index = key_attrs[k];
                    auto sink_col_index = fk->GetSinkColumnIds()[k];
                    src_new_tuple.SetValue(src_col_index,
                                            new_tuple.GetValue(sink_col_index),
                                            executor_context_->GetPool());
                  }

                  ItemPointer new_loc = src_table->InsertEmptyVersion();

                  if (new_loc.IsNull()) {
                    if (src_is_owner == false) {
                      transaction_manager.YieldOwnership(current_txn,
                                                         src_tile_group_header,
                                                         ptr->offset);
                    }
                    transaction_manager.SetTransactionResult(current_txn,
                                                             peloton::ResultType::FAILURE);
                    return false;
                  }

                  transaction_manager.PerformDelete(current_txn,
                                                    *ptr,
                                                    new_loc);

                  ItemPointer *index_entry_ptr = nullptr;
                  peloton::ItemPointer location =
                      src_table->InsertTuple(&src_new_tuple, current_txn, &index_entry_ptr, false);

                  if (location.block == INVALID_OID) {
                    transaction_manager.SetTransactionResult(current_txn,
                                                             peloton::ResultType::FAILURE);
                    return false;
                  }

                  transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);

                  break;
                }
              }
            }
          }

          break;
        }
      }
    }
  }

  // Check the sink tables of foreign key constraints
  fk_count = target_table_->GetForeignKeyCount();
  if (fk_count > 0) {
    LOG_DEBUG("asdasdas\n");
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

  auto executor_pool = executor_context_->GetPool();
  auto target_table_schema = target_table_->GetSchema();
  auto column_count = target_table_schema->GetColumnCount();

  trigger::TriggerList *trigger_list = target_table_->GetTriggerList();
  if (trigger_list != nullptr) {
    LOG_TRACE("size of trigger list in target table: %d",
              trigger_list->GetTriggerListSize());
    if (trigger_list->HasTriggerType(TriggerType::BEFORE_UPDATE_STATEMENT)) {
      LOG_TRACE("target table has per-statement-before-update triggers!");
      trigger_list->ExecTriggers(TriggerType::BEFORE_UPDATE_STATEMENT,
                                 current_txn);
    }
  }

  // Update tuples in a given table
  for (oid_t visible_tuple_id : *source_tile) {
    storage::TileGroup *tile_group =
        source_tile->GetBaseTile(0)->GetTileGroup();
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
          current_txn, tile_group_header, physical_tuple_id,
          VisibilityIdType::COMMIT_ID);
      if (visibility != VisibilityType::OK) {
        transaction_manager.SetTransactionResult(current_txn,
                                                 ResultType::FAILURE);
        return false;
      }
    }
    ///////////////////////////////////////////////////////////

    if (trigger_list != nullptr) {
      LOG_TRACE("size of trigger list in target table: %d",
                trigger_list->GetTriggerListSize());
      if (trigger_list->HasTriggerType(TriggerType::BEFORE_UPDATE_ROW)) {
        LOG_TRACE("target table has per-row-before-update triggers!");
        trigger_list->ExecTriggers(TriggerType::BEFORE_INSERT_ROW, current_txn);
      }
    }

    bool is_owner = transaction_manager.IsOwner(current_txn, tile_group_header,
                                                physical_tuple_id);

    bool is_written = transaction_manager.IsWritten(
        current_txn, tile_group_header, physical_tuple_id);

    // Prepare to examine primary key
    bool ret = false;
    const planner::UpdatePlan &update_node = GetPlanNode<planner::UpdatePlan>();

    // if the current transaction is the creator of this version.
    // which means the current transaction has already updated the version.
    if (is_owner == true && is_written == true) {
      if (update_node.GetUpdatePrimaryKey()) {
        // Update primary key
        ret = PerformUpdatePrimaryKey(is_owner, tile_group, tile_group_header,
                                      physical_tuple_id, old_location);
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
        ContainerTuple<storage::TileGroup> old_tuple(tile_group,
                                                     physical_tuple_id);
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
      bool is_ownable = is_owner ||
                        transaction_manager.IsOwnable(
                            current_txn, tile_group_header, physical_tuple_id);

      if (is_ownable == true) {
        // if the tuple is not owned by any transaction and is visible to
        // current transaction.

        bool acquire_ownership_success =
            is_owner ||
            transaction_manager.AcquireOwnership(current_txn, tile_group_header,
                                                 physical_tuple_id);

        if (acquire_ownership_success == false) {
          LOG_TRACE("Fail to insert new tuple. Set txn failure.");
          transaction_manager.SetTransactionResult(current_txn,
                                                   ResultType::FAILURE);
          return false;
        }

        if (update_node.GetUpdatePrimaryKey()) {
          // Update primary key
          ret = PerformUpdatePrimaryKey(is_owner, tile_group, tile_group_header,
                                        physical_tuple_id, old_location);

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

          ContainerTuple<storage::TileGroup> new_tuple(new_tile_group.get(),
                                                       new_location.offset);

          ContainerTuple<storage::TileGroup> old_tuple(tile_group,
                                                       physical_tuple_id);

          // perform projection from old version to new version.
          // this triggers in-place update, and we do not need to allocate
          // another version.
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
              transaction_manager.YieldOwnership(current_txn, tile_group_header,
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

          // execute after-update-row triggers and
          // record on-commit-update-row triggers into current transaction
          if (trigger_list != nullptr) {
            LOG_TRACE("size of trigger list in target table: %d",
                      trigger_list->GetTriggerListSize());
            if (trigger_list->HasTriggerType(TriggerType::AFTER_UPDATE_ROW) ||
                trigger_list->HasTriggerType(
                    TriggerType::ON_COMMIT_UPDATE_ROW)) {
              std::unique_ptr<storage::Tuple> real_old_tuple(
                  new storage::Tuple(target_table_schema, true));
              std::unique_ptr<storage::Tuple> real_new_tuple(
                  new storage::Tuple(target_table_schema, true));
              for (oid_t column_itr = 0; column_itr < column_count;
                   column_itr++) {
                type::Value val = (old_tuple.GetValue(column_itr));
                real_old_tuple->SetValue(column_itr, val, executor_pool);
              }
              for (oid_t column_itr = 0; column_itr < column_count;
                   column_itr++) {
                type::Value val = (new_tuple.GetValue(column_itr));
                real_new_tuple->SetValue(column_itr, val, executor_pool);
              }

              if (trigger_list->HasTriggerType(TriggerType::AFTER_UPDATE_ROW)) {
                LOG_TRACE("target table has per-row-after-update triggers!");
                trigger_list->ExecTriggers(TriggerType::AFTER_UPDATE_ROW,
                                           current_txn, real_new_tuple.get(),
                                           executor_context_,
                                           real_old_tuple.get());
              }
              if (trigger_list->HasTriggerType(
                      TriggerType::ON_COMMIT_UPDATE_ROW)) {
                LOG_TRACE(
                    "target table has per-row-on-commit-update triggers!");
                trigger_list->ExecTriggers(TriggerType::ON_COMMIT_UPDATE_ROW,
                                           current_txn, real_new_tuple.get(),
                                           executor_context_,
                                           real_old_tuple.get());
              }
            }
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

  // execute after-update-statement triggers and
  // record on-commit-update-statement triggers into current transaction
  if (trigger_list != nullptr) {
    LOG_TRACE("size of trigger list in target table: %d",
              trigger_list->GetTriggerListSize());
    if (trigger_list->HasTriggerType(TriggerType::AFTER_UPDATE_STATEMENT)) {
      LOG_TRACE("target table has per-statement-after-update triggers!");
      trigger_list->ExecTriggers(TriggerType::AFTER_UPDATE_STATEMENT,
                                 current_txn);
    }
    if (trigger_list->HasTriggerType(TriggerType::ON_COMMIT_UPDATE_STATEMENT)) {
      LOG_TRACE("target table has per-statement-on-commit-update triggers!");
      trigger_list->ExecTriggers(TriggerType::ON_COMMIT_UPDATE_STATEMENT,
                                 current_txn);
    }
  }
  return true;
}

}  // namespace executor
}  // namespace peloton
