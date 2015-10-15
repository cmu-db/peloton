//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_executor.cpp
//
// Identification: src/backend/executor/insert_executor.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/insert_executor.h"

#include "backend/planner/insert_plan.h"
#include "backend/catalog/manager.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile.h"
#include "backend/storage/tuple_iterator.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/expression/container_tuple.h"

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

  const planner::InsertPlan &node = GetPlanNode<planner::InsertPlan>();
  storage::DataTable *target_table_ = node.GetTable();
  oid_t bulk_insert_count = node.GetBulkInsertCount();
  assert(target_table_);

  auto transaction_ = executor_context_->GetTransaction();

  // Inserting a logical tile.
  if (children_.size() == 1) {
    LOG_INFO("Insert executor :: 1 child \n");

    if (!children_[0]->Execute()) {
      return false;
    }

    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());
    assert(logical_tile.get() != nullptr);
    auto target_table_schema = target_table_->GetSchema();
    auto column_count = target_table_schema->GetColumnCount();

    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(target_table_schema, true));

    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      expression::ContainerTuple<LogicalTile> cur_tuple(logical_tile.get(), tuple_id);

      // Materialize the logical tile tuple
      for(oid_t column_itr = 0 ; column_itr < column_count ; column_itr++)
        tuple->SetValue(column_itr, cur_tuple.GetValue(column_itr));

      peloton::ItemPointer location = target_table_->InsertTuple(transaction_, tuple.get());
      if (location.block == INVALID_OID) {
        transaction_->SetResult(peloton::Result::RESULT_FAILURE);
        return false;
      }
      transaction_->RecordInsert(location);

      executor_context_->num_processed += 1; // insert one
    }

    return true;
  }
  // Inserting a collection of tuples from plan node
  else if (children_.size() == 0) {
    LOG_INFO("Insert executor :: 0 child \n");

    // Extract expressions from plan node and construct the tuple.
    // For now we just handle a single tuple
    auto schema = target_table_->GetSchema();
    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
    auto project_info = node.GetProjectInfo();

    // There should be no direct maps
    assert(project_info);
    assert(project_info->GetDirectMapList().size() == 0);

    for (auto target : project_info->GetTargetList()) {
      peloton::Value value =
          target.second->Evaluate(nullptr, nullptr, executor_context_);
      tuple->SetValue(target.first, value);
    }

    // Bulk Insert Mode
    for(oid_t insert_itr = 0; insert_itr < bulk_insert_count; insert_itr++) {
    
      // Carry out insertion
      ItemPointer location = target_table_->InsertTuple(transaction_, tuple.get());
      LOG_INFO("Inserted into location: %d, %d", location.block, location.offset);

      if (location.block == INVALID_OID) {
        LOG_INFO("Failed to Insert. Set txn failure.");
        transaction_->SetResult(peloton::Result::RESULT_FAILURE);
        return false;
      }
      transaction_->RecordInsert(location);

      // Logging
      {
        auto& log_manager = logging::LogManager::GetInstance();

        if(log_manager.IsInLoggingMode()){
          auto logger = log_manager.GetBackendLogger();
          auto record = logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_INSERT,
                                               transaction_->GetTransactionId(),
                                               target_table_->GetOid(),
                                               location,
                                               INVALID_ITEMPOINTER,
                                               tuple.get());

          logger->Log(record);
        }
      }

    }

    executor_context_->num_processed += 1; // insert one
    done_ = true;
    return true;
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
