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
#include "commands/trigger.h"
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
        transaction_manager.SetTransactionResult(
            current_txn, peloton::ResultType::FAILURE);
        return false;
      }

      transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);

      executor_context_->num_processed += 1;  // insert one
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
    std::unique_ptr<storage::Tuple> project_tuple;

    // Check if this is not a raw tuple
    if (tuple == nullptr) {
      // Otherwise, there must exist a project info
      PL_ASSERT(project_info);
      // There should be no direct maps
      PL_ASSERT(project_info->GetDirectMapList().size() == 0);

      project_tuple.reset(new storage::Tuple(schema, true));

      for (auto target : project_info->GetTargetList()) {
        auto value =
            target.second.expr->Evaluate(nullptr, nullptr, executor_context_);
        project_tuple->SetValue(target.first, value, executor_pool);
      }

      // Set tuple to point to temporary project tuple
      tuple = project_tuple.get();
    }

    // Bulk Insert Mode
    for (oid_t insert_itr = 0; insert_itr < bulk_insert_count; insert_itr++) {
      // if we are doing a bulk insert from values not project_info
      if (!project_info) {
        tuple = node.GetTuple(insert_itr);
      }

      // DEPRECATED! do not get trigger list from target table member variable;
      // instead, get it from the trigger catalog table
      commands::TriggerList* trigger_list = target_table->GetTriggerList();

      // // check whether there are per-row-before-insert triggers on this table using trigger catalog
      // oid_t database_oid = target_table->GetDatabaseOid();
      // LOG_INFO("database_oid = %d", database_oid);
      // LOG_INFO("table name=%s", target_table->GetName().c_str());
      // oid_t table_oid = catalog::TableCatalog::GetInstance()->GetTableOid(target_table->GetName(), database_oid, current_txn);
      // LOG_INFO("table_oid = %d", table_oid);
      // commands::TriggerList* trigger_list = catalog::TriggerCatalog::GetInstance()->GetTriggersByType(database_oid, table_oid, 
      //                         peloton::commands::EnumTriggerType::BEFORE_INSERT_ROW, current_txn);
      // LOG_INFO("reach here safely");

      auto new_tuple = tuple;
      if (trigger_list != nullptr) {
        LOG_INFO("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());

        if (trigger_list->HasTriggerType(commands::EnumTriggerType::BEFORE_INSERT_ROW)) {
          LOG_INFO("target table has per-row-before-insert triggers!");
          LOG_INFO("address of the origin tuple before firing triggers: 0x%lx", long(tuple));
          new_tuple = trigger_list->ExecBRInsertTriggers(const_cast<storage::Tuple *> (tuple));
          LOG_INFO("address of the new tuple after firing triggers: 0x%lx", long(new_tuple));
        }
      }

      if (new_tuple == nullptr) {
        // trigger doesn't allow this tuple to be inserted
        LOG_INFO("this tuple is rejected by trigger");
        continue;
      }

      // Carry out insertion
      ItemPointer *index_entry_ptr = nullptr;
      ItemPointer location =
          target_table->InsertTuple(new_tuple, current_txn, &index_entry_ptr);
      LOG_DEBUG("Inserted into location: %u, %u", location.block,
                location.offset);
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


      // // debug code below
      // oid_t database_oid = target_table->GetDatabaseOid();
      // LOG_INFO("database_oid = %d", database_oid);
      // LOG_INFO("table name=%s", target_table->GetName().c_str());
      // if (catalog::TriggerCatalog::GetInstance() == nullptr) {
      //   LOG_INFO("trigger catalog is null");
      // } else {
      //   LOG_INFO("trigger catalog is not null!");
      // }
      // if (catalog::TableCatalog::GetInstance() == nullptr) {
      //   LOG_INFO("table catalog is null!!!");
      // } else {
      //   LOG_INFO("table catalog is not null");
      // }
      // oid_t table_oid = catalog::TableCatalog::GetInstance()->GetTableOid(target_table->GetName(), database_oid, current_txn);
      // LOG_INFO("table_oid = %d", table_oid);
      // trigger_list = catalog::TriggerCatalog::GetInstance()->GetTriggersByType(database_oid, table_oid, 
      //                         peloton::commands::EnumTriggerType::BEFORE_INSERT_ROW, current_txn);
      // LOG_INFO("reach here safely");

    }

    done_ = true;
    return true;
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
