//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_executor.cpp
//
// Identification: src/executor/insert_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/insert_executor.h"

#include "catalog/manager.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/logical_tile.h"
#include "executor/executor_context.h"
#include "common/container_tuple.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"
#include "storage/tuple_iterator.h"
#include "trigger/trigger.h"
#include "storage/tuple.h"
#include "catalog/catalog.h"
#include "catalog/trigger_catalog.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for insert executor.
 * @param node Insert node corresponding to this executor.
 */
InsertExecutor::InsertExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DInit() {
  PL_ASSERT(children_.size() == 0 || children_.size() == 1);
  PL_ASSERT(executor_context_);

  done_ = false;
  return true;
}

/**
 * @brief Adds a column to the logical tile, using the position lists.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DExecute() {
  if (done_) return false;

  PL_ASSERT(!done_);
  PL_ASSERT(executor_context_ != nullptr);

  const planner::InsertPlan &node = GetPlanNode<planner::InsertPlan>();
  storage::DataTable *target_table = node.GetTable();
  oid_t bulk_insert_count = node.GetBulkInsertCount();

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  if (!target_table) {
    transaction_manager.SetTransactionResult(current_txn,
                                             peloton::ResultType::FAILURE);
    return false;
  }

  LOG_TRACE("Number of tuples in table before insert: %lu",
            target_table->GetTupleCount());
  auto executor_pool = executor_context_->GetPool();

  trigger::TriggerList *trigger_list = target_table->GetTriggerList();
  if (trigger_list != nullptr) {
    LOG_TRACE("size of trigger list in target table: %d",
              trigger_list->GetTriggerListSize());
    if (trigger_list->HasTriggerType(TriggerType::BEFORE_INSERT_STATEMENT)) {
      LOG_TRACE("target table has per-statement-before-insert triggers!");
      trigger_list->ExecTriggers(TriggerType::BEFORE_INSERT_STATEMENT,
                                 current_txn);
    }
  }

  // Inserting a logical tile.
  if (children_.size() == 1) {
    if (!children_[0]->Execute()) {
      return false;
    }

    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());

    // FIXME: Wrong? What if the result of select is nothing? Michael
    PL_ASSERT(logical_tile.get() != nullptr);

    auto target_table_schema = target_table->GetSchema();
    auto column_count = target_table_schema->GetColumnCount();

    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(target_table_schema, true));

    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      ContainerTuple<LogicalTile> cur_tuple(logical_tile.get(), tuple_id);

      // Materialize the logical tile tuple
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
        type::Value val = (cur_tuple.GetValue(column_itr));
        tuple->SetValue(column_itr, val, executor_pool);
      }

      // insert tuple into the table.
      ItemPointer *index_entry_ptr = nullptr;
      peloton::ItemPointer location =
          target_table->InsertTuple(tuple.get(), current_txn, &index_entry_ptr);

      // it is possible that some concurrent transactions have inserted the same
      // tuple.
      // in this case, abort the transaction.
      if (location.block == INVALID_OID) {
        transaction_manager.SetTransactionResult(current_txn,
                                                 peloton::ResultType::FAILURE);
        return false;
      }

      transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);

      executor_context_->num_processed += 1;  // insert one
    }

    // execute after-insert-statement triggers and
    // record on-commit-insert-statement triggers into current transaction
    if (trigger_list != nullptr) {
      LOG_TRACE("size of trigger list in target table: %d",
                trigger_list->GetTriggerListSize());
      if (trigger_list->HasTriggerType(TriggerType::AFTER_INSERT_STATEMENT)) {
        LOG_TRACE("target table has per-statement-after-insert triggers!");
        trigger_list->ExecTriggers(TriggerType::AFTER_INSERT_STATEMENT,
                                   current_txn);
      }
      if (trigger_list->HasTriggerType(
              TriggerType::ON_COMMIT_INSERT_STATEMENT)) {
        LOG_TRACE("target table has per-statement-on-commit-insert triggers!");
        trigger_list->ExecTriggers(TriggerType::ON_COMMIT_INSERT_STATEMENT,
                                   current_txn);
      }
    }
    return true;
  }
  // Inserting a collection of tuples from plan node
  else if (children_.size() == 0) {
    // Extract expressions from plan node and construct the tuple.
    // For now we just handle a single tuple
    auto schema = target_table->GetSchema();
    auto project_info = node.GetProjectInfo();
    auto tuple = node.GetTuple(0);
    std::unique_ptr<storage::Tuple> storage_tuple;

    // Check if this is not a raw tuple
    if (project_info) {
      // Otherwise, there must exist a project info
      PL_ASSERT(project_info);
      // There should be no direct maps
      PL_ASSERT(project_info->GetDirectMapList().size() == 0);

      storage_tuple.reset(new storage::Tuple(schema, true));

      for (auto target : project_info->GetTargetList()) {
        auto value =
            target.second.expr->Evaluate(nullptr, nullptr, executor_context_);
        storage_tuple->SetValue(target.first, value, executor_pool);
      }

      // Set tuple to point to temporary project tuple
      tuple = storage_tuple.get();
    }

    // Bulk Insert Mode
    for (oid_t insert_itr = 0; insert_itr < bulk_insert_count; insert_itr++) {
      // if we are doing a bulk insert from values not project_info

      if (!project_info) {
        tuple = node.GetTuple(insert_itr);

        if (tuple == nullptr) {
          storage_tuple.reset(new storage::Tuple(schema, true));

          // read from values
          uint32_t num_columns = schema->GetColumnCount();
          for (uint32_t col_id = 0; col_id < num_columns; col_id++) {
            auto value = node.GetValue(col_id + insert_itr * num_columns);
            storage_tuple->SetValue(col_id, value, executor_pool);
          }

          // Set tuple to point to temporary project tuple
          tuple = storage_tuple.get();
        }
      }

      trigger::TriggerList *trigger_list = target_table->GetTriggerList();

      auto new_tuple = tuple;
      if (trigger_list != nullptr) {
        LOG_TRACE("size of trigger list in target table: %d",
                  trigger_list->GetTriggerListSize());
        if (trigger_list->HasTriggerType(TriggerType::BEFORE_INSERT_ROW)) {
          LOG_TRACE("target table has per-row-before-insert triggers!");
          LOG_TRACE("address of the origin tuple before firing triggers: 0x%lx",
                    long(tuple));
          trigger_list->ExecTriggers(TriggerType::BEFORE_INSERT_ROW,
                                     current_txn,
                                     const_cast<storage::Tuple *>(tuple),
                                     executor_context_, nullptr, &new_tuple);
          LOG_TRACE("address of the new tuple after firing triggers: 0x%lx",
                    long(new_tuple));
        }
      }

      if (new_tuple == nullptr) {
        // trigger doesn't allow this tuple to be inserted
        LOG_TRACE("this tuple is rejected by trigger");
        continue;
      }

      // Carry out insertion
      ItemPointer *index_entry_ptr = nullptr;
      ItemPointer location =
          target_table->InsertTuple(new_tuple, current_txn, &index_entry_ptr);
      if (new_tuple->GetColumnCount() > 2) {
        type::Value val = (new_tuple->GetValue(2));
        LOG_TRACE("value: %s", val.GetInfo().c_str());
      }

      if (location.block == INVALID_OID) {
        LOG_TRACE("Failed to Insert. Set txn failure.");
        transaction_manager.SetTransactionResult(current_txn,
                                                 ResultType::FAILURE);
        return false;
      }

      transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);

      LOG_TRACE("Number of tuples in table after insert: %lu",
                target_table->GetTupleCount());

      executor_context_->num_processed += 1;  // insert one

      // execute after-insert-row triggers and
      // record on-commit-insert-row triggers into current transaction
      new_tuple = tuple;
      if (trigger_list != nullptr) {
        LOG_TRACE("size of trigger list in target table: %d",
                  trigger_list->GetTriggerListSize());
        if (trigger_list->HasTriggerType(TriggerType::AFTER_INSERT_ROW)) {
          LOG_TRACE("target table has per-row-after-insert triggers!");
          LOG_TRACE("address of the origin tuple before firing triggers: 0x%lx",
                    long(tuple));
          trigger_list->ExecTriggers(TriggerType::AFTER_INSERT_ROW, current_txn,
                                     const_cast<storage::Tuple *>(tuple),
                                     executor_context_, nullptr, &new_tuple);
          LOG_TRACE("address of the new tuple after firing triggers: 0x%lx",
                    long(new_tuple));
        }
        if (trigger_list->HasTriggerType(TriggerType::ON_COMMIT_INSERT_ROW)) {
          LOG_TRACE("target table has per-row-on-commit-insert triggers!");
          LOG_TRACE("address of the origin tuple before firing triggers: 0x%lx",
                    long(tuple));
          trigger_list->ExecTriggers(TriggerType::ON_COMMIT_INSERT_ROW,
                                     current_txn,
                                     const_cast<storage::Tuple *>(tuple),
                                     executor_context_, nullptr, &new_tuple);
          LOG_TRACE("address of the new tuple after firing triggers: 0x%lx",
                    long(new_tuple));
        }
      }
    }
    // execute after-insert-statement triggers and
    // record on-commit-insert-statement triggers into current transaction
    trigger_list = target_table->GetTriggerList();
    if (trigger_list != nullptr) {
      LOG_TRACE("size of trigger list in target table: %d",
                trigger_list->GetTriggerListSize());
      if (trigger_list->HasTriggerType(TriggerType::AFTER_INSERT_STATEMENT)) {
        LOG_TRACE("target table has per-statement-after-insert triggers!");
        trigger_list->ExecTriggers(TriggerType::AFTER_INSERT_STATEMENT,
                                   current_txn);
      }
      if (trigger_list->HasTriggerType(
              TriggerType::ON_COMMIT_INSERT_STATEMENT)) {
        LOG_TRACE("target table has per-statement-on-commit-insert triggers!");
        trigger_list->ExecTriggers(TriggerType::ON_COMMIT_INSERT_STATEMENT,
                                   current_txn);
      }
    }
    done_ = true;
    return true;
  }
  return true;
}

}  // namespace executor
}  // namespace peloton
