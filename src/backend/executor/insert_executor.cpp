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
#include "backend/logging/logmanager.h"
#include "backend/logging/records/tuplerecord.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for insert executor.
 * @param node Insert node corresponding to this executor.
 */
InsertExecutor::InsertExecutor(planner::AbstractPlan *node,
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
  assert(target_table_);

  auto transaction_ = executor_context_->GetTransaction();

  // Inserting a logical tile.
  if (children_.size() == 1) {
    LOG_TRACE("Insert executor :: 1 child \n");

    if (!children_[0]->Execute()) {
      return false;
    }

    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());
    assert(logical_tile.get() != nullptr);

    // Get the underlying physical tile
    storage::Tile *physical_tile = logical_tile.get()->GetBaseTile(0);

    // Next, check logical tile schema against table schema
    auto schema = target_table_->GetSchema();
    const catalog::Schema *tile_schema = physical_tile->GetSchema();

    if (*schema != *tile_schema) {
      LOG_ERROR(
          "Insert executor :: table schema and logical tile schema don't match "
          "\n");
      return false;
    }

    storage::TupleIterator tile_iterator = physical_tile->GetIterator();
    storage::Tuple tuple(physical_tile->GetSchema());

    while (tile_iterator.Next(tuple)) {
      peloton::ItemPointer location = target_table_->InsertTuple(transaction_, &tuple);
      if (location.block == INVALID_OID) {
        transaction_->SetResult(peloton::Result::RESULT_FAILURE);
        return false;
      }
      transaction_->RecordInsert(location);
   }

    executor_context_->num_processed += 1; // insert one
    return true;
  }
  // Inserting a collection of tuples from plan node
  else if (children_.size() == 0) {
    LOG_TRACE("Insert executor :: 0 child \n");

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

    // Carry out insertion
    ItemPointer location = target_table_->InsertTuple(transaction_, tuple.get());
    LOG_INFO("location: %d, %d", location.block, location.offset);

    if (location.block == INVALID_OID) {
      transaction_->SetResult(peloton::Result::RESULT_FAILURE);
      return false;
    }
    transaction_->RecordInsert(location);

    // Logging 
    {
      auto& logManager = logging::LogManager::GetInstance();

      if(logManager.IsReadyToLogging()){
        auto logger = logManager.GetBackendLogger();
        auto record = logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_INSERT,
                                             transaction_->GetTransactionId(), 
                                             target_table_->GetOid(),
                                             location,
                                             INVALID_ITEMPOINTER,
                                             tuple.get());

        logger->log(record);
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
