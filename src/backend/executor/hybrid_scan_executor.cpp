//
// Created by Rui Wang on 16-4-29.
//

#include "hybrid_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>

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
  const planner::HybridScanPlan &node = GetPlanNode<planner::HybridScanPlan>();

  table_ = node.GetDataTable();
  index_ = node.GetDataIndex();

  if (table_ != nullptr && index_ == nullptr) {
    type_ = SEQ;

    current_tile_group_offset_ = START_OID;

    if (table_ != nullptr) {
      table_tile_group_count_ = table_->GetTileGroupCount();

      if (column_ids_.empty()) {
        column_ids_.resize(table_->GetSchema()->GetColumnCount());
        std::iota(column_ids_.begin(), column_ids_.end(), 0);
      }
    }

  } else if (table_ == nullptr && index_ != nullptr) {
    type_ = INDEX;
  } else {
    type_ = HYBRID;
  }

  return true;
}

bool HybridScanExecutor::DExecute() {
  if (type_ == SEQ) {
    // assume do not read from a logical tile.
    assert(children_.size() == 0);

    LOG_TRACE("Hybrid executor, Seq Scan  :: 0 child ");

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
      std::vector<oid_t> position_list;
      for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {

        ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

        // check transaction visibility
        if (transaction_manager.IsVisible(tile_group_header, tuple_id)) {
          // if the tuple is visible, then perform predicate evaluation.
          if (predicate_ == nullptr) {
            position_list.push_back(tuple_id);
            auto res = transaction_manager.PerformRead(location);
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
              auto res = transaction_manager.PerformRead(location);
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
