//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_executor.cpp
//
// Identification: src/executor/insert_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/insert_executor.h"

#include "catalog/manager.h"
#include "common/logger.h"
#include "common/varlen_pool.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/logical_tile.h"
#include "executor/executor_context.h"
#include "expression/container_tuple.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"
#include "storage/tuple_iterator.h"

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

  if(!target_table) {
	  transaction_manager.SetTransactionResult(current_txn, peloton::Result::RESULT_FAILURE);
	         return false;
  }

  LOG_TRACE("Number of tuples in table before insert: %lu",
            target_table->GetTupleCount());
  auto executor_pool = executor_context_->GetExecutorContextPool();

  // Inserting a logical tile.
  if (children_.size() == 1) {
    LOG_TRACE("Insert executor :: 1 child ");

    if (!children_[0]->Execute()) {
      return false;
    }

    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());
    PL_ASSERT(logical_tile.get() != nullptr);
    auto target_table_schema = target_table->GetSchema();
    auto column_count = target_table_schema->GetColumnCount();

    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(target_table_schema, true));

    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      expression::ContainerTuple<LogicalTile> cur_tuple(logical_tile.get(),
                                                        tuple_id);

      // Materialize the logical tile tuple
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
        common::Value val = (cur_tuple.GetValue(column_itr));
        tuple->SetValue(column_itr, val, executor_pool);
      }

      // insert tuple into the table.
      ItemPointer *index_entry_ptr = nullptr;
      peloton::ItemPointer location = target_table->InsertTuple(tuple.get(), current_txn, &index_entry_ptr);

      // it is possible that some concurrent transactions have inserted the same tuple.
      // in this case, abort the transaction.
      if (location.block == INVALID_OID) {
        transaction_manager.SetTransactionResult(current_txn, peloton::Result::RESULT_FAILURE);
        return false;
      }

      transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);

      executor_context_->num_processed += 1;  // insert one
    }

    return true;
  }
  // Inserting a collection of tuples from plan node
  else if (children_.size() == 0) {
    LOG_TRACE("Insert executor :: 0 child ");

    // Extract expressions from plan node and construct the tuple.
    // For now we just handle a single tuple
    auto schema = target_table->GetSchema();
    auto project_info = node.GetProjectInfo();
    auto tuple = node.GetTuple();
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
            target.second->Evaluate(nullptr, nullptr, executor_context_);
        project_tuple->SetValue(target.first, value, executor_pool);
      }

      // Set tuple to point to temporary project tuple
      tuple = project_tuple.get();
    }

    // Bulk Insert Mode
    for (oid_t insert_itr = 0; insert_itr < bulk_insert_count; insert_itr++) {

      // Carry out insertion
      ItemPointer *index_entry_ptr = nullptr;
      ItemPointer location = target_table->InsertTuple(tuple, current_txn, &index_entry_ptr);
      LOG_TRACE("Inserted into location: %u, %u", location.block,
                location.offset);
      if (tuple->GetColumnCount() > 2) {
        common::Value val = (tuple->GetValue(2));
        LOG_TRACE("value: %s", val.GetInfo().c_str());
      }

      if (location.block == INVALID_OID) {
        LOG_TRACE("Failed to Insert. Set txn failure.");
        transaction_manager.SetTransactionResult(current_txn, Result::RESULT_FAILURE);
        return false;
      }

      transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);
      
      LOG_TRACE("Number of tuples in table after insert: %lu",
                target_table->GetTupleCount());

      executor_context_->num_processed += 1;  // insert one
    }

    done_ = true;
    return true;
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
