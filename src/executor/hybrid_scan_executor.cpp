//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hybrid_scan_executor.cpp
//
// Identification: src/executor/hybrid_scan_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/container_tuple.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/hybrid_scan_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "planner/hybrid_scan_plan.h"
#include "storage/data_table.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "common/internal_types.h"

namespace peloton {
namespace executor {

HybridScanExecutor::HybridScanExecutor(const planner::AbstractPlan *node,
                                       ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context),
      indexed_tile_offset_(START_OID) {}

bool HybridScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  const planner::HybridScanPlan &node = GetPlanNode<planner::HybridScanPlan>();

  table_ = node.GetTable();
  index_ = node.GetDataIndex();
  type_ = node.GetHybridType();
  PL_ASSERT(table_ != nullptr);

  // SEQUENTIAL SCAN
  if (type_ == HybridScanType::SEQUENTIAL) {
    LOG_TRACE("Sequential Scan");
    current_tile_group_offset_ = START_OID;

    table_tile_group_count_ = table_->GetTileGroupCount();
    if (column_ids_.empty()) {
      column_ids_.resize(table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }
  // INDEX SCAN
  else if (type_ == HybridScanType::INDEX) {
    LOG_TRACE("Index Scan");
    index_ = node.GetIndex();

    result_itr_ = START_OID;
    index_done_ = false;
    result_.clear();

    PL_ASSERT(index_ != nullptr);
    column_ids_ = node.GetColumnIds();
    auto key_column_ids_ = node.GetKeyColumnIds();
    auto expr_types_ = node.GetExprTypes();
    values_ = node.GetValues();
    auto runtime_keys_ = node.GetRunTimeKeys();
    predicate_ = node.GetPredicate();
    key_ready_ = false;

    if (runtime_keys_.size() != 0) {
      assert(runtime_keys_.size() == values_.size());

      if (!key_ready_) {
        values_.clear();

        for (auto expr : runtime_keys_) {
          auto value = expr->Evaluate(nullptr, nullptr, executor_context_);
          LOG_TRACE("Evaluated runtime scan key: %s", value.GetInfo().c_str());
          values_.push_back(value.Copy());
        }

        key_ready_ = true;
      }
    }

    if (table_ != nullptr) {
      LOG_TRACE("Column count : %lu", table_->GetSchema()->GetColumnCount());
      full_column_ids_.resize(table_->GetSchema()->GetColumnCount());
      std::iota(full_column_ids_.begin(), full_column_ids_.end(), 0);
    }
  }
  // HYBRID SCAN
  else if (type_ == HybridScanType::HYBRID) {
    LOG_TRACE("Hybrid Scan");

    table_tile_group_count_ = table_->GetTileGroupCount();
    int offset = index_->GetIndexedTileGroupOff();
    indexed_tile_offset_ = (offset == -1) ? INVALID_OID : (oid_t)offset;
    block_threshold = 0;

    if (indexed_tile_offset_ == INVALID_OID) {
      current_tile_group_offset_ = START_OID;
    } else {
      current_tile_group_offset_ = indexed_tile_offset_ + 1;
      std::shared_ptr<storage::TileGroup> tile_group;
      if (current_tile_group_offset_ < table_tile_group_count_) {
        tile_group = table_->GetTileGroup(current_tile_group_offset_);
      } else {
        tile_group = table_->GetTileGroup(table_tile_group_count_ - 1);
      }

      oid_t tuple_id = 0;
      ItemPointer location(tile_group->GetTileGroupId(), tuple_id);
      block_threshold = location.block;
    }

    result_itr_ = START_OID;
    index_done_ = false;
    result_.clear();

    column_ids_ = node.GetColumnIds();
    auto key_column_ids_ = node.GetKeyColumnIds();
    auto expr_types_ = node.GetExprTypes();
    values_ = node.GetValues();
    auto runtime_keys_ = node.GetRunTimeKeys();
    predicate_ = node.GetPredicate();

    if (runtime_keys_.size() != 0) {
      assert(runtime_keys_.size() == values_.size());

      if (!key_ready_) {
        values_.clear();

        for (auto expr : runtime_keys_) {
          auto value = expr->Evaluate(nullptr, nullptr, executor_context_);
          LOG_TRACE("Evaluated runtime scan key: %s", value.GetInfo().c_str());
          values_.push_back(value.Copy());
        }

        key_ready_ = true;
      }
    }

    if (table_ != nullptr) {
      full_column_ids_.resize(table_->GetSchema()->GetColumnCount());
      std::iota(full_column_ids_.begin(), full_column_ids_.end(), 0);
    }
  }
  // FALLBACK
  else {
    throw Exception("Invalid hybrid scan type : " + HybridScanTypeToString(type_));
  }

  return true;
}

bool HybridScanExecutor::SeqScanUtil() {
  assert(children_.size() == 0);
  // LOG_TRACE("Hybrid executor, Seq Scan :: 0 child");

  assert(table_ != nullptr);
  assert(column_ids_.size() > 0);

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();

  // Retrieve next tile group.
  while (current_tile_group_offset_ < table_tile_group_count_) {
    LOG_TRACE("Current tile group offset : %u", current_tile_group_offset_);
    auto tile_group = table_->GetTileGroup(current_tile_group_offset_++);
    auto tile_group_header = tile_group->GetHeader();

    oid_t active_tuple_count = tile_group->GetNextTupleSlot();

    // Construct position list by looping through tile group
    // and applying the predicate.
    oid_t upper_bound_block = 0;
    if (item_pointers_.size() > 0) {
      auto reverse_iter = item_pointers_.rbegin();
      upper_bound_block = reverse_iter->block;
    }

    std::vector<oid_t> position_list;
    for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      ItemPointer location(tile_group->GetTileGroupId(), tuple_id);
      if (type_ == HybridScanType::HYBRID && item_pointers_.size() > 0 &&
          location.block <= upper_bound_block) {
        if (item_pointers_.find(location) != item_pointers_.end()) {
          continue;
        }
      }

      // Check transaction visibility
      if (transaction_manager.IsVisible(current_txn, tile_group_header,
                                        tuple_id) == VisibilityType::OK) {
        // If the tuple is visible, then perform predicate evaluation.
        if (predicate_ == nullptr) {
          position_list.push_back(tuple_id);
        } else {
          ContainerTuple<storage::TileGroup> tuple(tile_group.get(), tuple_id);
          auto eval =
              predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
          if (eval == true) {
            position_list.push_back(tuple_id);
          }
        }
      } else {
        ContainerTuple<storage::TileGroup> tuple(tile_group.get(), tuple_id);
        auto eval =
            predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
        if (eval == true) {
          position_list.push_back(tuple_id);
          auto res = transaction_manager.PerformRead(current_txn, location,
                                                     acquire_owner);
          if (!res) {
            transaction_manager.SetTransactionResult(current_txn,
                                                     ResultType::FAILURE);
            return res;
          }
        }
      }
    }

    // Don't return empty tiles
    if (position_list.size() == 0) {
      continue;
    }

    // Construct logical tile.
    std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
    logical_tile->AddColumns(tile_group, column_ids_);
    logical_tile->AddPositionList(std::move(position_list));

    LOG_TRACE("Hybrid executor, Seq Scan :: Got a logical tile");
    SetOutput(logical_tile.release());

    return true;
  }

  return false;
}

bool HybridScanExecutor::IndexScanUtil() {
  // Already performed the index lookup
  assert(index_done_);

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

bool HybridScanExecutor::DExecute() {
  // SEQUENTIAL SCAN
  if (type_ == HybridScanType::SEQUENTIAL) {
    LOG_TRACE("Sequential Scan");
    return SeqScanUtil();
  }
  // INDEX SCAN
  else if (type_ == HybridScanType::INDEX) {
    LOG_TRACE("Index Scan");
    PL_ASSERT(children_.size() == 0);

    if (index_done_ == false) {
      if (index_->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
        auto status = ExecPrimaryIndexLookup();
        if (status == false) {
          return false;
        }
      } else {
        return false;
      }
    }

    return IndexScanUtil();
  }
  // HYBRID SCAN
  else if (type_ == HybridScanType::HYBRID) {
    LOG_TRACE("Hybrid Scan");

    // do two part search
    if (index_done_ == false) {
      if (indexed_tile_offset_ == INVALID_OID) {
        index_done_ = true;
      } else {
        ExecPrimaryIndexLookup();
        LOG_TRACE("Using index -- tile count : %lu", result_.size());
      }
    }

    if (IndexScanUtil() == true) {
      return true;
    }
    // Scan seq
    return SeqScanUtil();
  }
  // FALLBACK
  else {
    throw Exception("Invalid hybrid scan type : " + HybridScanTypeToString(type_));
  }
}

bool HybridScanExecutor::ExecPrimaryIndexLookup() {
  PL_ASSERT(index_done_ == false);

  const planner::HybridScanPlan &node = GetPlanNode<planner::HybridScanPlan>();
  bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();

  auto key_column_ids_ = node.GetKeyColumnIds();
  auto expr_type_ = node.GetExprTypes();

  std::vector<ItemPointer *> tuple_location_ptrs;

  PL_ASSERT(index_->GetIndexType() == IndexConstraintType::PRIMARY_KEY);

  if (0 == key_column_ids_.size()) {
    LOG_TRACE("Scan all keys");
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    LOG_TRACE("Scan");
    index_->Scan(values_, key_column_ids_, expr_type_,
                 ScanDirectionType::FORWARD, tuple_location_ptrs,
                 &node.GetIndexPredicate().GetConjunctionList()[0]);
  }

  LOG_TRACE("Result tuple count: %lu", tuple_location_ptrs.size());

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  if (tuple_location_ptrs.size() == 0) {
    index_done_ = true;
    return false;
  }

  std::map<oid_t, std::vector<oid_t>> visible_tuples;

  // for every tuple that is found in the index.
  for (auto tuple_location_ptr : tuple_location_ptrs) {
    ItemPointer tuple_location = *tuple_location_ptr;

    if (type_ == HybridScanType::HYBRID &&
        tuple_location.block >= (block_threshold)) {
      item_pointers_.insert(tuple_location);
    }

    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();

    // perform transaction read
    size_t chain_length = 0;
    while (true) {
      ++chain_length;

      auto visibility = transaction_manager.IsVisible(
          current_txn, tile_group_header, tuple_location.offset);

      if (visibility == VisibilityType::OK) {
        visible_tuples[tuple_location.block].push_back(tuple_location.offset);
        auto res = transaction_manager.PerformRead(current_txn, tuple_location,
                                                   acquire_owner);
        if (!res) {
          transaction_manager.SetTransactionResult(current_txn,
                                                   ResultType::FAILURE);
          return res;
        }
        break;
      } else {
        ItemPointer old_item = tuple_location;
        cid_t old_end_cid = tile_group_header->GetEndCommitId(old_item.offset);

        tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);
        // there must exist a visible version.
        assert(tuple_location.IsNull() == false);

        cid_t max_committed_cid = transaction_manager.GetExpiredCid();

        // check whether older version is garbage.
        if (old_end_cid < max_committed_cid) {
          assert(tile_group_header->GetTransactionId(old_item.offset) ==
                     INITIAL_TXN_ID ||
                 tile_group_header->GetTransactionId(old_item.offset) ==
                     INVALID_TXN_ID);

          if (tile_group_header->SetAtomicTransactionId(
                  old_item.offset, INVALID_TXN_ID) == true) {
            // atomically swap item pointer held in the index bucket.
            AtomicUpdateItemPointer(tuple_location_ptr, tuple_location);
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

  index_done_ = true;

  LOG_TRACE("Result tiles : %lu", result_.size());

  return true;
}

}  // namespace executor
}  // namespace peloton
