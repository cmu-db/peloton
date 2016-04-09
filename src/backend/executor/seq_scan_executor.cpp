//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.cpp
//
// Identification: src/backend/executor/seq_scan_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/seq_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/common/logger.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
SeqScanExecutor::SeqScanExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base class DInit() first, then do mine.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  // Grab data from plan node.
  const planner::SeqScanPlan &node = GetPlanNode<planner::SeqScanPlan>();

  target_table_ = node.GetTable();

  current_tile_group_offset_ = START_OID;

  if (target_table_ != nullptr) {
    table_tile_group_count_ = target_table_->GetTileGroupCount();

    if (column_ids_.empty()) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }

  return true;
}

/**
 * @brief Creates logical tile from tile group and applies scan predicate.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DExecute() {
  // Scanning over a logical tile.
  if (children_.size() == 1) {
    // FIXME Check all requirements for children_.size() == 0 case.
    LOG_TRACE("Seq Scan executor :: 1 child ");

    assert(target_table_ == nullptr);
    assert(column_ids_.size() == 0);

    while (children_[0]->Execute()) {
      std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

      if (predicate_ != nullptr) {
        // Invalidate tuples that don't satisfy the predicate.
        for (oid_t tuple_id : *tile) {
          expression::ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
          if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
                  .IsFalse()) {
            tile->RemoveVisibility(tuple_id);
          }
        }
      }

      if (0 == tile->GetTupleCount()) {  // Avoid returning empty tiles
        continue;
      }

      /* Hopefully we needn't do projections here */
      SetOutput(tile.release());
      return true;
    }

    return false;
  }
  // Scanning a table
  else if (children_.size() == 0) {
    LOG_TRACE("Seq Scan executor :: 0 child ");

    assert(target_table_ != nullptr);
    assert(column_ids_.size() > 0);

    auto &transaction_manager =
        concurrency::TransactionManagerFactory::GetInstance();
    // Retrieve next tile group.
    while (current_tile_group_offset_ < table_tile_group_count_) {
      auto tile_group =
          target_table_->GetTileGroup(current_tile_group_offset_++);
      auto tile_group_header = tile_group->GetHeader();

      oid_t active_tuple_count = tile_group->GetNextTupleSlot();

      // Print tile group visibility
      // tile_group_header->PrintVisibility(txn_id, commit_id);

      // Construct position list by looping through tile group
      // and applying the predicate.
      std::vector<oid_t> position_list;
      for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
        // txn_id_t tuple_txn_id =
        // tile_group_header->GetTransactionId(tuple_id);
        // cid_t tuple_begin_cid =
        // tile_group_header->GetBeginCommitId(tuple_id);
        // cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

        // check transaction visibility
        if (transaction_manager.IsVisible(tile_group_header, tuple_id)) {
          // if the tuple is visible, then perform predicate evaluation.
          if (predicate_ == nullptr) {
            position_list.push_back(tuple_id);
            auto res = transaction_manager.PerformRead(
                tile_group->GetTileGroupId(), tuple_id);
            if (!res) {
              transaction_manager.SetTransactionResult(RESULT_FAILURE);
              return res;
            }
          } else {
            expression::ContainerTuple<storage::TileGroup> tuple(
                tile_group.get(), tuple_id);
            auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_)
                            .IsTrue();
            if (eval == true) {
              position_list.push_back(tuple_id);
              auto res = transaction_manager.PerformRead(
                  tile_group->GetTileGroupId(), tuple_id);
              if (!res) {
                transaction_manager.SetTransactionResult(RESULT_FAILURE);
                return res;
              }
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

      SetOutput(logical_tile.release());
      return true;
    }
  }

  return false;
}

}  // namespace executor
}  // namespace peloton
