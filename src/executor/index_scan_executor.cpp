//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_executor.cpp
//
// Identification: src/executor/index_scan_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/index_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/executor_context.h"
#include "expression/abstract_expression.h"
#include "expression/container_tuple.h"
#include "index/index.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/logger.h"
#include "catalog/manager.h"
#include "gc/gc_manager_factory.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for indexscan executor.
 * @param node Indexscan node corresponding to this executor.
 */
IndexScanExecutor::IndexScanExecutor(const planner::AbstractPlan *node,
                                     ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

IndexScanExecutor::~IndexScanExecutor() {
  // Nothing to do here
}

/**
 * @brief Let base class Dinit() first, then do my job.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  PL_ASSERT(children_.size() == 0);

  // Grab info from plan node and check it
  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  index_ = node.GetIndex();
  PL_ASSERT(index_ != nullptr);

  result_itr_ = START_OID;
  result_.clear();
  done_ = false;
  key_ready_ = false;

  column_ids_ = node.GetColumnIds();
  key_column_ids_ = node.GetKeyColumnIds();
  expr_types_ = node.GetExprTypes();
  values_ = node.GetValues();
  runtime_keys_ = node.GetRunTimeKeys();
  predicate_ = node.GetPredicate();

  if (runtime_keys_.size() != 0) {
    PL_ASSERT(runtime_keys_.size() == values_.size());

    if (!key_ready_) {
      values_.clear();

      for (auto expr : runtime_keys_) {
        auto value = expr->Evaluate(nullptr, nullptr, executor_context_);
        LOG_TRACE("Evaluated runtime scan key: %s", value.GetInfo().c_str());
        values_.push_back(value);
      }

      key_ready_ = true;
    }
  }

  table_ = node.GetTable();

  if (table_ != nullptr) {
    full_column_ids_.resize(table_->GetSchema()->GetColumnCount());
    std::iota(full_column_ids_.begin(), full_column_ids_.end(), 0);
  }

  return true;
}

/**
 * @brief Creates logical tile(s) after scanning index.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DExecute() {
  LOG_TRACE("Index Scan executor :: 0 child");

  if (!done_) {
    if (index_->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
      auto status = ExecPrimaryIndexLookup();
      if (status == false) return false;
    } else {
      auto status = ExecSecondaryIndexLookup();
      if (status == false) return false;
    }
  }
  // Already performed the index lookup
  PL_ASSERT(done_);

  while (result_itr_ < result_.size()) {  // Avoid returning empty tiles
    if (result_[result_itr_]->GetTupleCount() == 0) {
      result_itr_++;
      continue;
    } else {
      LOG_TRACE("Information %s", result_[result_itr_]->GetInfo().c_str());
      SetOutput(result_[result_itr_]);
      result_itr_++;
      return true;
    }

  }  // end while

  return false;
}

bool IndexScanExecutor::ExecPrimaryIndexLookup() {
  LOG_TRACE("Exec primary index lookup");
  assert(!done_);

  std::vector<ItemPointer *> tuple_location_ptrs;

  // Grab info from plan node
  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  auto column_ids_ = node.GetColumnIds();
  auto key_column_ids_ = node.GetKeyColumnIds();
  auto expr_types_ = node.GetExprTypes();

  assert(index_->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY);

  if (0 == key_column_ids_.size()) {
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    index_->Scan(values_,
                 key_column_ids_,
                 expr_types_,
                 SCAN_DIRECTION_TYPE_FORWARD,
                 tuple_location_ptrs,
                 &node.GetIndexPredicate().GetConjunctionList()[0]);
  }


  if (tuple_location_ptrs.size() == 0) {
    LOG_TRACE("no tuple is retrieved from index.");
    return false;
  }

  auto &transaction_manager =
    concurrency::TransactionManagerFactory::GetInstance();

  std::map<oid_t, std::vector<oid_t>> visible_tuples;

  // for every tuple that is found in the index.
  for (auto tuple_location_ptr : tuple_location_ptrs) {

    ItemPointer tuple_location = *tuple_location_ptr;

    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();

    size_t chain_length = 0;

    while (true) {
      ++chain_length;

      auto visibility = transaction_manager.IsVisible(tile_group_header, tuple_location.offset);

      // if the tuple is deleted
      if (visibility == VISIBILITY_DELETED) {
        LOG_TRACE("encounter deleted tuple: %u, %u", tuple_location.block, tuple_location.offset);
        break;
      }
        // if the tuple is visible.
      else if (visibility == VISIBILITY_OK) {
        LOG_TRACE("perform read: %u, %u", tuple_location.block,
                 tuple_location.offset);

        // perform predicate evaluation.
        if (predicate_ == nullptr) {
          visible_tuples[tuple_location.block].push_back(tuple_location.offset);

          auto res = transaction_manager.PerformRead(tuple_location);
          if (!res) {
            transaction_manager.SetTransactionResult(RESULT_FAILURE);
            return res;
          }
          
        } else {
          expression::ContainerTuple<storage::TileGroup> tuple(
            tile_group.get(), tuple_location.offset);
          auto eval =
            predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
          if (eval == true) {
            visible_tuples[tuple_location.block].push_back(tuple_location.offset);

            auto res = transaction_manager.PerformRead(tuple_location);
            if (!res) {
              transaction_manager.SetTransactionResult(RESULT_FAILURE);
              return res;
            }
            
          }
        }
        break;
      }
        // if the tuple is not visible.
      else {

        // Break for new to old
        if (tile_group_header->GetTransactionId(tuple_location.offset) == INITIAL_TXN_ID
          && tile_group_header->GetEndCommitId(tuple_location.offset) <= concurrency::current_txn->GetBeginCommitId()) {
          // See an invisible version that does not belong to any one in a new to old version chain
          // Wire back
          // tuple_location = *(transaction_manager.GetHeadPtr(tile_group_header, tuple_location.offset));
          tuple_location = *(tile_group_header->GetIndirection(tuple_location.offset));
          tile_group = manager.GetTileGroup(tuple_location.block);
          tile_group_header = tile_group.get()->GetHeader();
          chain_length = 0;
          continue;
        }

        ItemPointer old_item = tuple_location;
        tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);


        // there must exist a visible version.
        if(tuple_location.IsNull()) {
          if (chain_length == 1) {
            break;
          }

          // If we have traversed through the chain and still can not fulfill one of the above conditions,
          // something wrong must happen.
          // For speculative read, a transaction may incidentally miss a visible tuple due to a non-atomic
          // timestamp update. In such case, we just return false and abort the txn.
          transaction_manager.SetTransactionResult(RESULT_FAILURE);
          return false;
        }


        // if it is vacuum GC or no GC.
        tile_group = manager.GetTileGroup(tuple_location.block);
        tile_group_header = tile_group.get()->GetHeader();
        continue;
      }
    }
  }


  // Construct a logical tile for each block
  for (auto tuples : visible_tuples) {
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuples.first);

    std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
    // Add relevant columns to logical tile
    logical_tile->AddColumns(tile_group, full_column_ids_);
    logical_tile->AddPositionList(std::move(tuples.second));
    if (column_ids_.size() != 0) {
      logical_tile->ProjectColumns(full_column_ids_, column_ids_);
    }

    result_.push_back(logical_tile.release());
  }

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result_.size());

  return true;
}


bool IndexScanExecutor::ExecSecondaryIndexLookup() {
  LOG_TRACE("ExecSecondaryIndexLookup");
  PL_ASSERT(!done_);

  std::vector<ItemPointer *> tuple_location_ptrs;

  // Grab info from plan node
  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  auto column_ids_ = node.GetColumnIds();
  auto key_column_ids_ = node.GetKeyColumnIds();
  auto expr_types_ = node.GetExprTypes();

  PL_ASSERT(index_->GetIndexType() != INDEX_CONSTRAINT_TYPE_PRIMARY_KEY);

  if (0 == key_column_ids_.size()) {
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    index_->Scan(values_, key_column_ids_, expr_types_,
                 SCAN_DIRECTION_TYPE_FORWARD, tuple_location_ptrs, &node.GetIndexPredicate().GetConjunctionList()[0]);
  }


  if (tuple_location_ptrs.size() == 0) {
    LOG_TRACE("no tuple is retrieved from index.");
    return false;
  }

  auto &transaction_manager =
    concurrency::TransactionManagerFactory::GetInstance();

  std::map<oid_t, std::vector<oid_t>> visible_tuples;

  for (auto tuple_location_ptr : tuple_location_ptrs) {
    ItemPointer tuple_location = *tuple_location_ptr;

    auto &manager = catalog::Manager::GetInstance();

    auto tile_group = manager.GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();

    size_t chain_length = 0;

    while (true) {
      // check the version chain
      ++chain_length;

      auto visibility = transaction_manager.IsVisible(tile_group_header, tuple_location.offset);

      if (visibility == VISIBILITY_DELETED) {
        LOG_TRACE("encounter deleted tuple: %u, %u", tuple_location.block, tuple_location.offset);
        break;
      }
      if (visibility == VISIBILITY_INVISIBLE) {
        LOG_TRACE("Invisible read: %u, %u", tuple_location.block,
                  tuple_location.offset);


        // Break for new to old
        if (tile_group_header->GetTransactionId(tuple_location.offset) == INITIAL_TXN_ID
            && tile_group_header->GetEndCommitId(tuple_location.offset) <= concurrency::current_txn->GetBeginCommitId()) {
          // See an invisible version that does not belong to any one in a new to old version chain
          // Wire back
          tuple_location = *(tile_group_header->GetIndirection(tuple_location.offset));
          tile_group = manager.GetTileGroup(tuple_location.block);
          tile_group_header = tile_group.get()->GetHeader();
          //chain_length = 0;
          continue;
        }

        ItemPointer old_item = tuple_location;
        tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);

        // there must exist a visible version.

        if(tuple_location.IsNull()) {
          // FIXME:
          // For an index scan on a version chain, the result should be one of the following:
          //    (1) find a visible version
          //    (2) find a deleted version
          //    (3) find an aborted version with chain length equal to one
          if (chain_length == 1) {
            break;
          }

          // If we have traversed through the chain and still can not fulfill one of the above conditions,
          // something wrong must happen.
          // For speculative read, a transaction may incidentally miss a visible tuple due to a non-atomic
          // timestamp update. In such case, we just return false and abort the txn.
          transaction_manager.SetTransactionResult(RESULT_FAILURE);
          return false;
        }

        tile_group = manager.GetTileGroup(tuple_location.block);
        tile_group_header = tile_group.get()->GetHeader();
      } else {
        PL_ASSERT(visibility == VISIBILITY_OK);
        LOG_TRACE("perform read: %u, %u", tuple_location.block,
                  tuple_location.offset);

        // Further check if the version has the secondary key
        storage::Tuple key_tuple(index_->GetKeySchema(), true);
        expression::ContainerTuple<storage::TileGroup> candidate_tuple(
          tile_group.get(), tuple_location.offset
        );
        // Construct the key tuple
        auto &indexed_columns = index_->GetKeySchema()->GetIndexedColumns();

        oid_t this_col_itr = 0;
        for (auto col : indexed_columns) {
          key_tuple.SetValue(this_col_itr, candidate_tuple.GetValue(col), index_->GetPool());
          this_col_itr++;
        }

        // Compare the key tuple and the key
        if (index_->Compare(key_tuple, key_column_ids_, expr_types_, values_) == false) {
          LOG_TRACE("Secondary key mismatch: %u, %u\n", tuple_location.block, tuple_location.offset);
          break;
        }

        // perform predicate evaluation.
        if (predicate_ == nullptr) {
          visible_tuples[tuple_location.block].push_back(tuple_location.offset);

          auto res = transaction_manager.PerformRead(tuple_location);
          if (!res) {
            transaction_manager.SetTransactionResult(RESULT_FAILURE);
            return res;
          }
          
        } else {
          expression::ContainerTuple<storage::TileGroup> tuple(
            tile_group.get(), tuple_location.offset);
          auto eval =
            predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
          if (eval == true) {
            visible_tuples[tuple_location.block].push_back(tuple_location.offset);

            auto res = transaction_manager.PerformRead(tuple_location);
            if (!res) {
              transaction_manager.SetTransactionResult(RESULT_FAILURE);
              return res;
            }
          }
        }
        break;
      }
    }
    LOG_TRACE("Traverse length: %d\n", (int)chain_length);
  }

  // Construct a logical tile for each block
  for (auto tuples : visible_tuples) {
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuples.first);

    std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
    // Add relevant columns to logical tile
    logical_tile->AddColumns(tile_group, full_column_ids_);
    logical_tile->AddPositionList(std::move(tuples.second));
    if (column_ids_.size() != 0) {
      logical_tile->ProjectColumns(full_column_ids_, column_ids_);
    }

    result_.push_back(logical_tile.release());
  }

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result_.size());

  return true;
}


}  // namespace executor
}  // namespace peloton
