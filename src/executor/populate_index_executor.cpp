//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// populate_index_executor.cpp
//
// Identification: src/executor/populate_index_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_set>

#include "common/logger.h"
#include "type/value.h"
#include "executor/logical_tile.h"
#include "executor/populate_index_executor.h"
#include "executor/executor_context.h"
#include "planner/populate_index_plan.h"
#include "expression/tuple_value_expression.h"
#include "storage/data_table.h"
#include "storage/tile.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager_factory.h"
#include "index/index.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
PopulateIndexExecutor::PopulateIndexExecutor(const planner::AbstractPlan *node,
                                             ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool PopulateIndexExecutor::DInit() {
  PELOTON_ASSERT(children_.size() == 1);
  PELOTON_ASSERT(executor_context_);

  // Initialize executor state
  const planner::PopulateIndexPlan &node =
      GetPlanNode<planner::PopulateIndexPlan>();
  target_table_ = node.GetTable();
  column_ids_ = node.GetColumnIds();
  index_name_ = node.GetIndexName();
  concurrent_ = node.GetConcurrent();
  done_ = false;

  return true;
}

bool PopulateIndexExecutor::DExecute() {
  LOG_TRACE("Populate Index Executor");
  PELOTON_ASSERT(executor_context_ != nullptr);
  auto current_txn = executor_context_->GetTransaction();
  auto executor_pool = executor_context_->GetPool();
  if (done_ == false) {

    // Build index with lock
    if (concurrent_ == false){
      PELOTON_ASSERT(children_.size() == 1);
      oid_t table_oid = target_table_->GetOid();
      // Lock the table to exclusive
      // for switching between blocking/non-blocking.
      concurrency::LockManager *lm = concurrency::LockManager::GetInstance();
      LOG_TRACE("Exclusive Lock: lock mamager address is %p, table oid is %u", (void *)lm, table_oid);
      bool lock_success = lm->LockExclusive(table_oid);
      concurrency::LockManager::SafeLock dummy;
      if (!lock_success) {
        LOG_TRACE("Cannot obtain lock for the table, abort!");
      }
      else{
        LOG_WARN("Exclusive lock success, will last until populate index is over.");
        dummy.Set(table_oid, concurrency::LockManager::SafeLock::EXCLUSIVE);
      }

      // Get the output from seq_scan
      while (children_[0]->Execute()) {
        child_tiles_.emplace_back(children_[0]->GetOutput());
      }

      if (child_tiles_.size() == 0) {
        LOG_TRACE("PopulateIndex Executor : -- no child tiles ");
      }

      auto target_table_schema = target_table_->GetSchema();

      std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(target_table_schema, true));

      // Go over the logical tile and insert in the index the values
      for (size_t child_tile_itr = 0; child_tile_itr < child_tiles_.size();
           child_tile_itr++) {
        auto tile = child_tiles_[child_tile_itr].get();

        // Go over all tuples in the logical tile
        for (oid_t tuple_id : *tile) {
          ContainerTuple<LogicalTile> cur_tuple(tile, tuple_id);
	      LOG_TRACE("Add tuple in index");

          // Materialize the logical tile tuple
          for (oid_t column_itr = 0; column_itr < column_ids_.size();
               column_itr++) {
            type::Value val = (cur_tuple.GetValue(column_itr));
            tuple->SetValue(column_ids_[column_itr], val, executor_pool);
          }

          ItemPointer location(tile->GetBaseTile(0)->GetTileGroup()->GetTileGroupId(),
                               tuple_id);

          // insert tuple into the index.
          ItemPointer *index_entry_ptr = nullptr;
          target_table_->InsertInIndex(tuple.get(), location, current_txn,
                                       &index_entry_ptr, index_name_);
        }
      }
    }
    // build index without lock
    else{

      LOG_TRACE("Non-blocking create index");

      // Get the output from seq_scan
      while (children_[0]->Execute()) {
        child_tiles_.emplace_back(children_[0]->GetOutput());
      }

      if (child_tiles_.size() == 0) {
        LOG_DEBUG("PopulateIndex Executor : -- no child tiles ");
      }

      auto target_table_schema = target_table_->GetSchema();

      std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(target_table_schema, true));

      std::shared_ptr<index::Index> idx = target_table_->GetIndexWithName(index_name_);
      if (idx == nullptr){
        LOG_DEBUG("create index error, cannot find the index");
      }

      // Go over the logical tile and insert in the index the values
      for (size_t child_tile_itr = 0; child_tile_itr < child_tiles_.size();
           child_tile_itr++) {
	    LOG_TRACE("Add values to Index");
        auto tile = child_tiles_[child_tile_itr].get();

        // Go over all tuples in the logical tile
        for (oid_t tuple_id : *tile) {
          ContainerTuple<LogicalTile> cur_tuple(tile, tuple_id);

          // Materialize the logical tile tuple
          for (oid_t column_itr = 0; column_itr < column_ids_.size();
               column_itr++) {
            type::Value val = (cur_tuple.GetValue(column_itr));
            tuple->SetValue(column_ids_[column_itr], val, executor_pool);
          }

          ItemPointer location(tile->GetBaseTile(0)->GetTileGroup()->GetTileGroupId(),
                               tuple_id);

          // insert tuple into the index.
          ItemPointer *index_entry_ptr = nullptr;
          std::pair<storage::Tuple, ItemPointer> tmp(*tuple, location);
          if (!idx->CheckDuplicate(tmp)){
            target_table_->InsertInIndex(tuple.get(), location, current_txn,
                                         &index_entry_ptr, index_name_);
          }
        }
      }
      idx->ResetPopulated();
    }


    done_ = true;
  }
  LOG_TRACE("Populate Index Executor : false -- done ");
  return false;
}

}  // namespace executor
}  // namespace peloton
