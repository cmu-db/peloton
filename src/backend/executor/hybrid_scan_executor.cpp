//
// Created by Rui Wang on 16-4-29.
//

#include "hybrid_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>
#include <string>
#include <unordered_map>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>

#include "backend/common/timer.h"
#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"
#include "backend/planner/hybrid_scan_plan.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/common/logger.h"

namespace peloton {
namespace executor {

HybridScanExecutor::HybridScanExecutor(const planner::AbstractPlan *node,
                     ExecutorContext *executor_context)
  : AbstractScanExecutor(node, executor_context), indexed_tile_offset_(START_OID) {}

bool HybridScanExecutor::DInit() {
 auto status = AbstractScanExecutor::DInit();

 if (!status) return false;

  const planner::HybridScanPlan &node = GetPlanNode<planner::HybridScanPlan>();

  table_ = node.GetTable();
  index_ = node.GetDataIndex();
  type_ = node.GetHybridType();

  if (type_ == planner::SEQ) {
    current_tile_group_offset_ = START_OID;
    if (table_ != nullptr) {
      table_tile_group_count_ = table_->GetTileGroupCount();
      if (column_ids_.empty()) {
        column_ids_.resize(table_->GetSchema()->GetColumnCount());
        std::iota(column_ids_.begin(), column_ids_.end(), 0);
      }
    }

  } else if (type_ == planner::INDEX) {
    result_itr_ = START_OID;
    index_done_ = false;

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

    if (table_ != nullptr) {
      full_column_ids_.resize(table_->GetSchema()->GetColumnCount());
      std::iota(full_column_ids_.begin(), full_column_ids_.end(), 0);
    }
  } else { // Hybrid type.
    table_tile_group_count_ = table_->GetTileGroupCount();
    int offset = index_->GetIndexedTileGroupOff();
    indexed_tile_offset_ = (offset == -1) ? INVALID_OID : (oid_t)offset;

    //printf("Current indexed_tile_offset %d, table tile group count %d\n",
    //         indexed_tile_offset_, table_tile_group_count_);

    if (indexed_tile_offset_ == INVALID_OID) {
      current_tile_group_offset_ = START_OID;
    } else {
      current_tile_group_offset_ = indexed_tile_offset_ + 1;
      std::shared_ptr<storage::TileGroup> tile_group; 
      if (current_tile_group_offset_ < table_tile_group_count_) {
        tile_group =
          table_->GetTileGroup(current_tile_group_offset_);
      } else {
        tile_group =
          table_->GetTileGroup(table_tile_group_count_ - 1);
      }
      
      oid_t tuple_id = 0;
      ItemPointer location(tile_group->GetTileGroupId(), tuple_id);
      block_threshold = location.block;
    }

    result_itr_ = START_OID;
    index_done_ = false;

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
    
    if (table_ != nullptr) {
      full_column_ids_.resize(table_->GetSchema()->GetColumnCount());
      std::iota(full_column_ids_.begin(), full_column_ids_.end(), 0);
    }
  }

  return true;
}

bool HybridScanExecutor::SeqScanUtil() {
  assert(children_.size() == 0);
  // LOG_INFO("Hybrid executor, Seq Scan :: 0 child");

  assert(table_ != nullptr);
  assert(column_ids_.size() > 0);

  auto &transaction_manager =
    concurrency::TransactionManagerFactory::GetInstance();

  // Retrieve next tile group.
  while (current_tile_group_offset_ < table_tile_group_count_) {
    auto tile_group =
      table_->GetTileGroup(current_tile_group_offset_++);
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
      if (type_ == planner::HYBRID &&
          item_pointers_.size() > 0 &&
          location.block <= upper_bound_block) {
        if (item_pointers_.find(location) != item_pointers_.end()) {
          continue;
        }
      }

      // check transaction visibility
      if (transaction_manager.IsVisible(tile_group_header, tuple_id)) {
        // if the tuple is visible, then perform predicate evaluation.
        if (predicate_ == nullptr) {
          position_list.push_back(tuple_id);
        } else {
          expression::ContainerTuple<storage::TileGroup> tuple(
            tile_group.get(), tuple_id);
          auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_)
            .IsTrue();
          if (eval == true) {
            position_list.push_back(tuple_id);
          }
        }
      } else {
          expression::ContainerTuple<storage::TileGroup> tuple(
            tile_group.get(), tuple_id);
          auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_)
            .IsTrue();
          if (eval == true) {
            position_list.push_back(tuple_id);
            auto res = transaction_manager.PerformRead(location);
            if (!res) {
              transaction_manager.SetTransactionResult(RESULT_FAILURE);
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
      LOG_INFO("Hybrid executor, Seq Scan :: Got a logical tile");
      SetOutput(logical_tile.release());
      // printf("Construct a logical tile in seq scan\n");
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
      // printf("Construct a logical tile in index scan\n");

      return true;
    }
  }  // end while
  return false;
}

bool HybridScanExecutor::DExecute() {
  if (type_ == planner::SEQ) {
      return SeqScanUtil();
  } else if (type_ == planner::INDEX) {
  //  LOG_INFO("Hybrrd Scan executor, Index Scan :: 0 child");
    assert(children_.size() == 0);
    if (!index_done_) {
      if (index_->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
        auto status = ExecPrimaryIndexLookup();
        if (status == false) return false;
      }
//      else {
//        auto status = ExecSecondaryIndexLookup();
//        if (status == false) return false;
//      }
    }

    return IndexScanUtil();
  } else {
    // do two part search
    if (index_done_ == false) {

      //Timer<> timer;
      //timer.Start();

      if (indexed_tile_offset_ == INVALID_OID) {
        index_done_ = true;
      } else {
        ExecPrimaryIndexLookup();
      }
      /*timer.Stop();
      double time_per_transaction = timer.GetDuration();
      printf(" %f\n", time_per_transaction);
      */
    }

    if (IndexScanUtil() == true) {
      return true;
    }
    // Scan seq
    return SeqScanUtil();
  }
}

bool HybridScanExecutor::ExecPrimaryIndexLookup() {
  assert(!index_done_);

  std::vector<ItemPointer *> tuple_location_ptrs;

  assert(index_->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY);
  // Timer<> timer;
  // timer.Start();
  if (0 == key_column_ids_.size()) {
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    index_->Scan(values_, key_column_ids_, expr_types_,
                 SCAN_DIRECTION_TYPE_FORWARD, tuple_location_ptrs);
  }

  LOG_INFO("Tuple_locations.size(): %lu", tuple_location_ptrs.size());

  auto &transaction_manager =
    concurrency::TransactionManagerFactory::GetInstance();
  //Timer<> timer;
  //timer.Start();
  
  //timer.Stop();
  //double time_per_transaction = timer.GetDuration();
  //printf(" %f\n", time_per_transaction);

   if (tuple_location_ptrs.size() == 0) {
   // printf("set size %lu current seq scan off %d\n", item_pointers_.size(), current_tile_group_offset_);
    //timer.Stop();
    //double time_per_transaction = timer.GetDuration();
    //printf(" %f\n", time_per_transaction);
    index_done_ = true;
    return false;
  }

  //std::set<oid_t> oid_ts;

  std::map<oid_t, std::vector<oid_t>> visible_tuples;
  // for every tuple that is found in the index.
  for (auto tuple_location_ptr : tuple_location_ptrs) {
    ItemPointer tuple_location = *tuple_location_ptr;

    if (type_ == planner::HYBRID &&
      tuple_location.block >= (block_threshold)) {
        item_pointers_.insert(tuple_location);
    //  oid_ts.insert(tuple_location.block);
    }

    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();

    // perform transaction read
    size_t chain_length = 0;
    while (true) {

      ++chain_length;

      if (transaction_manager.IsVisible(tile_group_header,
                                        tuple_location.offset)) {
        visible_tuples[tuple_location.block].push_back(tuple_location.offset);
        auto res = transaction_manager.PerformRead(tuple_location);
        if (!res) {
          transaction_manager.SetTransactionResult(RESULT_FAILURE);
          return res;
        }
        break;
      } else {
        ItemPointer old_item = tuple_location;
        cid_t old_end_cid = tile_group_header->GetEndCommitId(old_item.offset);

        tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);
        // there must exist a visible version.
        assert(tuple_location.IsNull() == false);

        cid_t max_committed_cid = transaction_manager.GetMaxCommittedCid();

        // check whether older version is garbage.
        if (old_end_cid < max_committed_cid) {
          assert(tile_group_header->GetTransactionId(old_item.offset) == INITIAL_TXN_ID ||
                 tile_group_header->GetTransactionId(old_item.offset) == INVALID_TXN_ID);

          if (tile_group_header->SetAtomicTransactionId(old_item.offset, INVALID_TXN_ID) == true) {


            // atomically swap item pointer held in the index bucket.
            AtomicUpdateItemPointer(tuple_location_ptr, tuple_location);

            // currently, let's assume only primary index exists.
            gc::GCManagerFactory::GetInstance().RecycleTupleSlot(
              table_->GetOid(), old_item.block, old_item.offset,
              max_committed_cid);
          }
        }

        tile_group = manager.GetTileGroup(tuple_location.block);
        tile_group_header = tile_group.get()->GetHeader();
      }
    }
  }

  /*printf("set size %lu current seq scan off %d, block threshlod %d\n", item_pointers_.size(), current_tile_group_offset_, block_threshold);
  for (auto t : oid_ts) {
    std::cout << "block  " << t  << std::endl;
  }*/

   //timer.Stop();
   //double time_per_transaction = timer.GetDuration();
   //printf(" %f\n", time_per_transaction);

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
