//
// Created by Rui Wang on 16-4-4.
//

#include "backend/concurrency/transaction_manager_factory.h"
#include "exchange_seq_scan_executor.h"
#include "backend/storage/tile_group_header.h"
#include "backend/expression/container_tuple.h"
#include "backend/planner/exchange_seq_scan_plan.h"
#include "backend/common/thread_manager.h"
#include "backend/executor/exchange_seq_scan_executor.h"
#include "backend/executor/parallel_seq_scan_task_response.h"
#include "logical_tile_factory.h"

namespace peloton {
namespace executor {

ExchangeSeqScanExecutor::ExchangeSeqScanExecutor(
    const planner::AbstractPlan *node, ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

bool ExchangeSeqScanExecutor::DInit() {
  parallelize_done_ = false;

  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  // Grab data from plan node.
  const planner::ExchangeSeqScanPlan &node = GetPlanNode<planner::ExchangeSeqScanPlan>();

  target_table_ = node.GetTable();

  current_tile_group_offset_ = START_OID;

  if (target_table_ != nullptr) {
    table_tile_group_count_ = target_table_->GetTileGroupCount();

    if (column_ids_.empty()) {
      LOG_INFO("Column count: %lu", target_table_->GetSchema()->GetColumnCount());
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }

  return true;
}

bool ExchangeSeqScanExecutor::DExecute() {
  LOG_INFO("Exchange Seq Scan executor:: start execute, children_size %lu",
           children_.size());

  if (!parallelize_done_) {
    // When exchange_seq_scan_executor reads from a table, just paralleling each
    // part of this table.
    if (children_.size() == 0) {
      assert(target_table_ != nullptr);
      assert(column_ids_.size() != 0);
      assert(current_tile_group_offset_ == START_OID);

      auto &transaction_manager =
        concurrency::TransactionManagerFactory::GetInstance();

      while (current_tile_group_offset_ < table_tile_group_count_) {
        LOG_TRACE(
            "ExchangeSeqScanExecutor :: submit task to thread pool, dealing "
            "with %lu tile.",
            current_tile_group_offset_);
        std::function<void()> f_seq_scan =
            std::bind(&ExchangeSeqScanExecutor::ThreadExecute, this,
                      current_tile_group_offset_,
                      &transaction_manager);
        ThreadManager::GetInstance().AddTask(f_seq_scan);

        current_tile_group_offset_++;
      }

      parallelize_done_ = true;
    } else {
      // When exchange_seq_scan_executor, should use a single thread to create
      // parallel tasks.
      // TODO: Use one single thead here to keep fetching logical tiles from
      // child_, ignore it now.
      assert(children_.size() == 1);
      LOG_TRACE("Exchange Seq Scan executor :: 1 child ");
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

        SetOutput(tile.release());
        return true;
      }
      return false;
    }
  }

  current_tile_group_offset_ = START_OID;

  while (current_tile_group_offset_ < table_tile_group_count_) {
    std::unique_ptr<AbstractParallelTaskResponse> response_ptr(queue_.Get());
    current_tile_group_offset_++;
    if (response_ptr->GetStatus() == HasRetValue) {
      SetOutput(response_ptr->GetOutput());
      return true;
    } else if (response_ptr->GetStatus() == NoRetValue) {
      continue;
    } else if (response_ptr->GetStatus() == Abort) {
      // need to wait for all tasks and return task.

      while (current_tile_group_offset_ < table_tile_group_count_) {
        queue_.Get();
        current_tile_group_offset_++;
      }
    }
  }
  return false;
}

void ExchangeSeqScanExecutor::ThreadExecute(oid_t assigned_tile_group_offset,
                                              concurrency::TransactionManager* transaction_manager) {
  LOG_INFO(
      "Parallel worker :: ExchangeSeqScanExecutor :: SeqScanThreadMain, "
      "executor: %s with assigned tile group offset %lu" ,
      GetRawNode()->GetInfo().c_str(), assigned_tile_group_offset);

  bool seq_failure = false;

  auto tile_group = target_table_->GetTileGroup(assigned_tile_group_offset);
  auto tile_group_header = tile_group->GetHeader();

  oid_t active_tuple_count = tile_group->GetNextTupleSlot();

  // Print tile group visibility
  // Construct position list by looping through tile group
  // and applying the predicate.
  std::vector<oid_t> position_list;
  for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
    // check transaction visibility
    if (transaction_manager->IsVisible(tile_group_header, tuple_id)) {
      // if the tuple is visible, then perform predicate evaluation.
      if (predicate_ == nullptr) {
        position_list.push_back(tuple_id);
        auto res = transaction_manager->PerformRead(tile_group->GetTileGroupId(),
                                                   tuple_id);
        if (!res) {
          transaction_manager->SetTransactionResult(RESULT_FAILURE);
          seq_failure = true;
          break;
        }
      } else {
        expression::ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                             tuple_id);
        auto eval =
            predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
        if (eval == true) {
          position_list.push_back(tuple_id);
          auto res = transaction_manager->PerformRead(
              tile_group->GetTileGroupId(), tuple_id);
          if (!res) {
            transaction_manager->SetTransactionResult(RESULT_FAILURE);
            seq_failure = true;
            break;
          }
        }
      }
    }
  }

  AbstractParallelTaskResponse *response = nullptr;

  if (seq_failure) {
    response = new ParallelSeqScanTaskResponse(Abort);
  } else {
    if (position_list.size() == 0) {
      response = new ParallelSeqScanTaskResponse(NoRetValue);
    } else {
      // Construct logical tile.
      std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
      logical_tile->AddColumns(tile_group, column_ids_);
      logical_tile->AddPositionList(std::move(position_list));

      response =
          new ParallelSeqScanTaskResponse(HasRetValue, logical_tile.release());
    }
  }

  queue_.Put(response);
}

}  // executor
}  // peloton
