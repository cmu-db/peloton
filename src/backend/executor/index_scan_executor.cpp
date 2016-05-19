//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_executor.cpp
//
// Identification: src/backend/executor/index_scan_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/index_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"
#include "backend/index/index.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/common/logger.h"
#include "backend/catalog/manager.h"
#include "backend/gc/gc_manager_factory.h"

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

  assert(children_.size() == 0);

  // Grab info from plan node and check it
  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  index_ = node.GetIndex();
  assert(index_ != nullptr);

  result_itr_ = START_OID;
  done_ = false;

  column_ids_ = node.GetColumnIds();
  key_column_ids_ = node.GetKeyColumnIds();
  expr_types_ = node.GetExprTypes();
  values_ = node.GetValues();
  runtime_keys_ = node.GetRunTimeKeys();
  predicate_ = node.GetPredicate();

  if (runtime_keys_.size() != 0) {
    assert(runtime_keys_.size() == values_.size());

    if (!key_ready_) {
      values_.clear();

      for (auto expr : runtime_keys_) {
        auto value = expr->Evaluate(nullptr, nullptr, executor_context_);
        LOG_INFO("Evaluated runtime scan key: %s", value.GetInfo().c_str());
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
  LOG_INFO("Index Scan executor :: 0 child");

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
  assert(done_);
  while (result_itr_ < result_.size()) {  // Avoid returning empty tiles
    if (result_[result_itr_]->GetTupleCount() == 0) {
      result_itr_++;
      continue;
    } else {
      SetOutput(result_[result_itr_]);
      result_itr_++;
      return true;
    }

  }  // end while
  return false;
}

bool IndexScanExecutor::ExecPrimaryIndexLookup() {
  assert(!done_);

  std::vector<ItemPointer *> tuple_location_ptrs;

  assert(index_->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY);

  if (0 == key_column_ids_.size()) {
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    index_->Scan(values_, key_column_ids_, expr_types_,
                 SCAN_DIRECTION_TYPE_FORWARD, tuple_location_ptrs);
  }


  if (tuple_location_ptrs.size() == 0) return false;

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::map<oid_t, std::vector<oid_t>> visible_tuples;

  // The deconstructor of the GC buffer
  // will automatically register garbage to GC manager
  gc::GCBuffer garbage_tuples(table_->GetOid());

  // for every tuple that is found in the index.
  for (auto tuple_location_ptr : tuple_location_ptrs) {

    ItemPointer tuple_location = *tuple_location_ptr;

    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();

    size_t chain_length = 0;

    cid_t max_committed_cid = transaction_manager.GetMaxCommittedCid();

    while (true) {
      ++chain_length;

      auto visibility = transaction_manager.IsVisible(tile_group_header, tuple_location.offset);

      // if the tuple is deleted
      if (visibility == VISIBILITY_DELETED) {
        LOG_INFO("encounter deleted tuple: %u, %u", tuple_location.block, tuple_location.offset);
        break;
      }
      // if the tuple is visible.
      else if (visibility == VISIBILITY_OK) {
        LOG_INFO("perform read: %u, %u", tuple_location.block,
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
            visible_tuples[tuple_location.block]
                .push_back(tuple_location.offset);

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
        ItemPointer old_item = tuple_location;
        tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);
        cid_t old_end_cid = tile_group_header->GetEndCommitId(old_item.offset);


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

        if(gc::GCManagerFactory::GetGCType() != GC_TYPE_CO){
          tile_group = manager.GetTileGroup(tuple_location.block);
          tile_group_header = tile_group.get()->GetHeader();
          continue;
        }

        // check whether older version is garbage.
        if (old_end_cid <= max_committed_cid) {
          assert(tile_group_header->GetTransactionId(old_item.offset) == INITIAL_TXN_ID
                 || tile_group_header->GetTransactionId(old_item.offset) == INVALID_TXN_ID);

          if (tile_group_header->SetAtomicTransactionId(old_item.offset, INVALID_TXN_ID) == true) {

            // atomically swap item pointer held in the index bucket.
            AtomicUpdateItemPointer(tuple_location_ptr, tuple_location);

            garbage_tuples.AddGarbage(old_item);

            tile_group = manager.GetTileGroup(tuple_location.block);
            tile_group_header = tile_group.get()->GetHeader();
            tile_group_header->SetPrevItemPointer(tuple_location.offset, INVALID_ITEMPOINTER);

            // Continue the while loop with the new header we get the index head ptr
            continue;
          }
        }
        tile_group = manager.GetTileGroup(tuple_location.block);
        tile_group_header = tile_group.get()->GetHeader();
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
  assert(!done_);

  std::vector<ItemPointer> tuple_locations;

  assert(index_->GetIndexType() != INDEX_CONSTRAINT_TYPE_PRIMARY_KEY);

  if (0 == key_column_ids_.size()) {
    index_->ScanAllKeys(tuple_locations);
  } else {
    index_->Scan(values_, key_column_ids_, expr_types_,
                 SCAN_DIRECTION_TYPE_FORWARD, tuple_locations);
  }

  LOG_INFO("Tuple_locations.size(): %lu", tuple_locations.size());

  if (tuple_locations.size() == 0) return false;

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::map<oid_t, std::vector<oid_t>> visible_tuples;
  // for every tuple that is found in the index.
  for (auto tuple_location : tuple_locations) {
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();
    auto tile_group_id = tuple_location.block;
    auto tuple_id = tuple_location.offset;

    // if the tuple is visible.
    if (transaction_manager.IsVisible(tile_group_header, tuple_id)) {
      // perform predicate evaluation.
      if (predicate_ == nullptr) {
        visible_tuples[tile_group_id].push_back(tuple_id);
        auto res = transaction_manager.PerformRead(tuple_location);
        if (!res) {
          transaction_manager.SetTransactionResult(RESULT_FAILURE);
          return res;
        }
      } else {
        expression::ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                             tuple_id);
        auto eval =
            predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
        if (eval == true) {
          visible_tuples[tile_group_id].push_back(tuple_id);
          auto res = transaction_manager.PerformRead(tuple_location);
          if (!res) {
            transaction_manager.SetTransactionResult(RESULT_FAILURE);
            return res;
          }
        }
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

}  // namespace executor
}  // namespace peloton
