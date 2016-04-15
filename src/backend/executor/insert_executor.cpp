//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_executor.cpp
//
// Identification: src/backend/executor/insert_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/insert_executor.h"

#include "backend/catalog/manager.h"
#include "backend/common/logger.h"
#include "backend/common/pool.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/container_tuple.h"
#include "backend/planner/insert_plan.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple_iterator.h"

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
  assert(children_.size() == 0 || children_.size() == 1);
  assert(executor_context_);

  done_ = false;
  return true;
}

/**
 * @brief Adds a column to the logical tile, using the position lists.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DExecute() {
  if (done_) return false;

  assert(!done_);
  assert(executor_context_ != nullptr);

  const planner::InsertPlan &node = GetPlanNode<planner::InsertPlan>();
  storage::DataTable *target_table = node.GetTable();
  oid_t bulk_insert_count = node.GetBulkInsertCount();
  assert(target_table);

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();
  auto executor_pool = executor_context_->GetExecutorContextPool();

  // Inserting a logical tile.
  if (children_.size() == 1) {
    LOG_TRACE("Insert executor :: 1 child ");

    if (!children_[0]->Execute()) {
      return false;
    }

    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());
    assert(logical_tile.get() != nullptr);
    auto target_table_schema = target_table->GetSchema();
    auto column_count = target_table_schema->GetColumnCount();

    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(target_table_schema, true));

    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      expression::ContainerTuple<LogicalTile> cur_tuple(logical_tile.get(),
                                                        tuple_id);

      // Materialize the logical tile tuple
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++)
        tuple->SetValue(column_itr, cur_tuple.GetValue(column_itr),
                        executor_pool);

      peloton::ItemPointer location = target_table->InsertTuple(tuple.get());
      if (location.block == INVALID_OID) {
        transaction_manager.SetTransactionResult(
            peloton::Result::RESULT_FAILURE);
        return false;
      }
      auto res =
          transaction_manager.PerformInsert(location.block, location.offset);
      if (!res) {
        transaction_manager.SetTransactionResult(RESULT_FAILURE);
        return res;
      }

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
    if(tuple == nullptr) {
      // Otherwise, there must exist a project info
      assert(project_info);
      // There should be no direct maps
      assert(project_info->GetDirectMapList().size() == 0);

      project_tuple.reset(new storage::Tuple(schema, true));

      for (auto target : project_info->GetTargetList()) {
        peloton::Value value =
            target.second->Evaluate(nullptr, nullptr, executor_context_);
        project_tuple->SetValue(target.first, value, executor_pool);
      }

      // Set tuple to point to temporary project tuple
      tuple = project_tuple.get();
    }

    // Bulk Insert Mode
    for (oid_t insert_itr = 0; insert_itr < bulk_insert_count; insert_itr++) {

      // Carry out insertion
      ItemPointer location = target_table->InsertTuple(tuple);
      LOG_TRACE("Inserted into location: %lu, %lu", location.block,
                location.offset);

      if (location.block == INVALID_OID) {
        LOG_TRACE("Failed to Insert. Set txn failure.");
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        return false;
      }

      auto res =
          transaction_manager.PerformInsert(location.block, location.offset);
      if (!res) {
        transaction_manager.SetTransactionResult(RESULT_FAILURE);
        return res;
      }

      executor_context_->num_processed += 1;  // insert one

    }

    done_ = true;
    return true;
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
