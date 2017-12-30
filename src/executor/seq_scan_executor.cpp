//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.cpp
//
// Identification: src/executor/seq_scan_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/seq_scan_executor.h"

#include "common/internal_types.h"
#include "type/value_factory.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/executor_context.h"
#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/comparison_expression.h"
#include "common/container_tuple.h"
#include "planner/create_plan.h"
#include "storage/data_table.h"
#include "storage/tile_group_header.h"
#include "storage/tile.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/logger.h"

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

  old_predicate_ = predicate_;

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
  if (children_.size() == 1 &&
      // There will be a child node on the create index scenario,
      // but we don't want to use this execution flow
      !(GetRawNode()->GetChildren().size() > 0 &&
        GetRawNode()->GetChildren()[0].get()->GetPlanNodeType() ==
            PlanNodeType::CREATE &&
        ((planner::CreatePlan *)GetRawNode()->GetChildren()[0].get())
                ->GetCreateType() == CreateType::INDEX)) {
    // FIXME Check all requirements for children_.size() == 0 case.
    LOG_TRACE("Seq Scan executor :: 1 child ");

    PL_ASSERT(target_table_ == nullptr);
    PL_ASSERT(column_ids_.size() == 0);

    while (children_[0]->Execute()) {
      std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

      if (predicate_ != nullptr) {
        // Invalidate tuples that don't satisfy the predicate.
        for (oid_t tuple_id : *tile) {
          ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
          auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_);
          if (eval.IsFalse()) {
            // if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
            //        .IsFalse()) {
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
  else if (children_.size() == 0 ||
           // If we are creating an index, there will be a child
           (children_.size() == 1 &&
            // This check is only needed to pass seq_scan_test
            // unless it is possible to add a executor child
            // without a corresponding plan.
            GetRawNode()->GetChildren().size() > 0 &&
            // Check if the plan is what we actually expect.
            GetRawNode()->GetChildren()[0].get()->GetPlanNodeType() ==
                PlanNodeType::CREATE &&
            // If it is, confirm it is for indexes
            ((planner::CreatePlan *)GetRawNode()->GetChildren()[0].get())
                    ->GetCreateType() == CreateType::INDEX)) {
    LOG_TRACE("Seq Scan executor :: 0 child ");

    PL_ASSERT(target_table_ != nullptr);
    PL_ASSERT(column_ids_.size() > 0);
    if (children_.size() > 0 && !index_done_) {
      children_[0]->Execute();
      // This stops continuous executions due to
      // a parent and avoids multiple creations
      // of the same index.
      index_done_ = true;
    }
    
    concurrency::TransactionManager &transaction_manager =
        concurrency::TransactionManagerFactory::GetInstance();

    bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();
    auto current_txn = executor_context_->GetTransaction();

    // Retrieve next tile group.
    while (current_tile_group_offset_ < table_tile_group_count_) {
      auto tile_group =
          target_table_->GetTileGroup(current_tile_group_offset_++);
      auto tile_group_header = tile_group->GetHeader();

      oid_t active_tuple_count = tile_group->GetNextTupleSlot();

      // Construct position list by looping through tile group
      // and applying the predicate.
      std::vector<oid_t> position_list;
      for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
        ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

        auto visibility = transaction_manager.IsVisible(
            current_txn, tile_group_header, tuple_id);

        // check transaction visibility
        if (visibility == VisibilityType::OK) {
          // if the tuple is visible, then perform predicate evaluation.
          if (predicate_ == nullptr) {
            position_list.push_back(tuple_id);
            auto res = transaction_manager.PerformRead(current_txn, location,
                                                       acquire_owner);
            if (!res) {
              transaction_manager.SetTransactionResult(current_txn,
                                                       ResultType::FAILURE);
              return res;
            }
          } else {
            ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                     tuple_id);
            LOG_TRACE("Evaluate predicate for a tuple");
            auto eval =
                predicate_->Evaluate(&tuple, nullptr, executor_context_);
            LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
            if (eval.IsTrue()) {
              position_list.push_back(tuple_id);
              auto res = transaction_manager.PerformRead(current_txn, location,
                                                         acquire_owner);
              if (!res) {
                transaction_manager.SetTransactionResult(current_txn,
                                                         ResultType::FAILURE);
                return res;
              } else {
                LOG_TRACE("Sequential Scan Predicate Satisfied");
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

      LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
      SetOutput(logical_tile.release());
      return true;
    }
  }

  return false;
}

// Update Predicate expression
// this is used in the NLJoin executor
void SeqScanExecutor::UpdatePredicate(const std::vector<oid_t> &column_ids,
                                      const std::vector<type::Value> &values) {
  std::vector<oid_t> predicate_column_ids;

  PL_ASSERT(column_ids.size() <= column_ids_.size());

  // columns_ids is the column id
  // in the join executor, should
  // convert to the column id in the
  // seq scan executor
  for (auto column_id : column_ids) {
    predicate_column_ids.push_back(column_ids_[column_id]);
  }

  expression::AbstractExpression *new_predicate =
      values.size() != 0 ? ColumnsValuesToExpr(predicate_column_ids, values, 0)
                         : nullptr;

  // combine with original predicate
  if (old_predicate_ != nullptr) {
    expression::AbstractExpression *lexpr = new_predicate,
                                   *rexpr = old_predicate_->Copy();

    new_predicate = new expression::ConjunctionExpression(
        ExpressionType::CONJUNCTION_AND, lexpr, rexpr);
  }

  // Currently a hack that prevent memory leak
  // we should eventually make prediate_ a unique_ptr
  new_predicate_.reset(new_predicate);
  predicate_ = new_predicate;
}

// Transfer a list of equality predicate
// to a expression tree
expression::AbstractExpression *SeqScanExecutor::ColumnsValuesToExpr(
    const std::vector<oid_t> &predicate_column_ids,
    const std::vector<type::Value> &values, size_t idx) {
  if (idx + 1 == predicate_column_ids.size())
    return ColumnValueToCmpExpr(predicate_column_ids[idx], values[idx]);

  // recursively build the expression tree
  expression::AbstractExpression *lexpr = ColumnValueToCmpExpr(
                                     predicate_column_ids[idx], values[idx]),
                                 *rexpr = ColumnsValuesToExpr(
                                     predicate_column_ids, values, idx + 1);

  expression::AbstractExpression *root_expr =
      new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND,
                                            lexpr, rexpr);

  root_expr->DeduceExpressionType();
  return root_expr;
}

expression::AbstractExpression *SeqScanExecutor::ColumnValueToCmpExpr(
    const oid_t column_id, const type::Value &value) {
  expression::AbstractExpression *lexpr =
      new expression::TupleValueExpression("");
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)->SetValueType(
      target_table_->GetSchema()->GetColumn(column_id).GetType());
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)
      ->SetValueIdx(column_id);

  expression::AbstractExpression *rexpr =
      new expression::ConstantValueExpression(value);

  expression::AbstractExpression *root_expr =
      new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, lexpr,
                                           rexpr);

  root_expr->DeduceExpressionType();
  return root_expr;
}
}  // namespace executor
}  // namespace peloton
