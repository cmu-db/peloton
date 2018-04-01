//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/optimizer/stats_calculator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats_calculator.h"

#include <cmath>

#include "catalog/table_catalog.h"
#include "expression/expression_util.h"
#include "expression/tuple_value_expression.h"
#include "optimizer/memo.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/table_stats.h"
#include "optimizer/stats/selectivity.h"
#include "optimizer/stats/stats_storage.h"

namespace peloton {
namespace optimizer {

void StatsCalculator::CalculateStats(GroupExpression *gexpr,
                                     ExprSet required_cols, Memo *memo,
                                     concurrency::TransactionContext *txn) {
  gexpr_ = gexpr;
  memo_ = memo;
  required_cols_ = required_cols;
  txn_ = txn;
  gexpr->Op().Accept(this);
}

void StatsCalculator::Visit(const LogicalGet *op) {
  if (op->table == nullptr) {
    // Dummy scan
    return;
  }
  auto table_stats = std::dynamic_pointer_cast<TableStats>(
      StatsStorage::GetInstance()->GetTableStats(
          op->table->GetDatabaseOid(), op->table->GetTableOid(), txn_));
  // First, get the required stats of the base table
  std::unordered_map<std::string, std::shared_ptr<ColumnStats>> required_stats;
  for (auto &col : required_cols_) {
    // Make a copy for required stats since we may want to modify later
    AddBaseTableStats(col, table_stats, required_stats, true);
  }
  auto root_group = memo_->GetGroupByID(gexpr_->GetGroupID());
  // Compute selectivity at the first time
  if (root_group->GetNumRows() == -1) {
    std::unordered_map<std::string, std::shared_ptr<ColumnStats>>
        predicate_stats;
    for (auto &annotated_expr : op->predicates) {
      auto predicate = annotated_expr.expr.get();
      ExprSet expr_set;
      expression::ExpressionUtil::GetTupleValueExprs(expr_set, predicate);
      for (auto &col : expr_set) {
        // Do not copy column stats in predicates since we do not need to modify
        AddBaseTableStats(col, table_stats, predicate_stats, false);
      }
    }
    // Use predicates to update the stats accordingly
    UpdateStatsForFilter(
        table_stats->GetColumnCount() == 0 ? 0 : table_stats->num_rows,
        predicate_stats, op->predicates);
  }
  // Add the stats to the group
  for (auto &column_name_stats_pair : required_stats) {
    auto &column_name = column_name_stats_pair.first;
    auto &column_stats = column_name_stats_pair.second;
    column_stats->num_rows = root_group->GetNumRows();
    memo_->GetGroupByID(gexpr_->GetGroupID())
        ->AddStats(column_name, column_stats);
  }
}

void StatsCalculator::Visit(const LogicalQueryDerivedGet *) {
  // TODO(boweic): Implement stats calculation for logical query derive get
  auto root_group = memo_->GetGroupByID(gexpr_->GetGroupID());
  root_group->SetNumRows(0);
  for (auto &col : required_cols_) {
    PELOTON_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto tv_expr = reinterpret_cast<expression::TupleValueExpression *>(col);
    auto bound_ids = tv_expr->GetBoundOid();
    root_group->AddStats(tv_expr->GetColFullName(),
                         std::make_shared<ColumnStats>(
                             std::get<0>(bound_ids), std::get<1>(bound_ids),
                             std::get<2>(bound_ids), tv_expr->GetColFullName(),
                             false, 0, 0.f, false, std::vector<double>{},
                             std::vector<double>{}, std::vector<double>{}));
  }
}

void StatsCalculator::Visit(const LogicalInnerJoin *op) {
  // Check if there's join condition
  PELOTON_ASSERT(gexpr_->GetChildrenGroupsSize() == 2);
  auto left_child_group = memo_->GetGroupByID(gexpr_->GetChildGroupId(0));
  auto right_child_group = memo_->GetGroupByID(gexpr_->GetChildGroupId(1));
  auto root_group = memo_->GetGroupByID(gexpr_->GetGroupID());
  // Calculate output num rows first
  if (root_group->GetNumRows() == -1) {
    size_t curr_rows =
        left_child_group->GetNumRows() * right_child_group->GetNumRows();
    for (auto &annotated_expr : op->join_predicates) {
      // See if there are join conditions
      if (annotated_expr.expr->GetExpressionType() ==
              ExpressionType::COMPARE_EQUAL &&
          annotated_expr.expr->GetChild(0)->GetExpressionType() ==
              ExpressionType::VALUE_TUPLE &&
          annotated_expr.expr->GetChild(1)->GetExpressionType() ==
              ExpressionType::VALUE_TUPLE) {
        auto left_child =
            reinterpret_cast<const expression::TupleValueExpression *>(
                annotated_expr.expr->GetChild(0));
        auto right_child =
            reinterpret_cast<const expression::TupleValueExpression *>(
                annotated_expr.expr->GetChild(1));
        if ((left_child_group->HasColumnStats(left_child->GetColFullName()) &&
             right_child_group->HasColumnStats(
                 right_child->GetColFullName())) ||
            (left_child_group->HasColumnStats(right_child->GetColFullName()) &&
             right_child_group->HasColumnStats(left_child->GetColFullName()))) {
          curr_rows /= std::max(std::max(left_child_group->GetNumRows(),
                                         right_child_group->GetNumRows()),
                                1);
        }
      }
    }
    root_group->SetNumRows(curr_rows);
  }
  size_t num_rows = root_group->GetNumRows();
  for (auto &col : required_cols_) {
    PELOTON_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto tv_expr = reinterpret_cast<expression::TupleValueExpression *>(col);
    std::shared_ptr<ColumnStats> column_stats;
    // Make a copy from the child stats
    if (left_child_group->HasColumnStats(tv_expr->GetColFullName())) {
      column_stats = std::make_shared<ColumnStats>(
          *left_child_group->GetStats(tv_expr->GetColFullName()));
    } else {
      PELOTON_ASSERT(right_child_group->HasColumnStats(tv_expr->GetColFullName()));
      column_stats = std::make_shared<ColumnStats>(
          *right_child_group->GetStats(tv_expr->GetColFullName()));
    }
    // Reset num_rows
    column_stats->num_rows = num_rows;
    root_group->AddStats(tv_expr->GetColFullName(), column_stats);
  }
  // TODO(boweic): calculate stats based on predicates other than join
  // conditions
}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalLeftJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalRightJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalOuterJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalSemiJoin *op) {}
void StatsCalculator::Visit(const LogicalAggregateAndGroupBy *) {
  // TODO(boweic): For now we just pass the stats needed without any
  // computation,
  // We need to implement stats computation for aggregation
  PELOTON_ASSERT(gexpr_->GetChildrenGroupsSize() == 1);
  // First, set num rows
  memo_->GetGroupByID(gexpr_->GetGroupID())
      ->SetNumRows(
          memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows());
  for (auto &col : required_cols_) {
    PELOTON_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto column_name = reinterpret_cast<expression::TupleValueExpression *>(col)
                           ->GetColFullName();
    memo_->GetGroupByID(gexpr_->GetGroupID())
        ->AddStats(column_name, memo_->GetGroupByID(gexpr_->GetChildGroupId(0))
                                    ->GetStats(column_name));
  }
}

void StatsCalculator::Visit(const LogicalLimit *op) {
  PELOTON_ASSERT(gexpr_->GetChildrenGroupsSize() == 1);
  auto group = memo_->GetGroupByID(gexpr_->GetGroupID());
  group->SetNumRows(std::min(
      (size_t)op->limit,
      (size_t)memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows()));
  for (auto &col : required_cols_) {
    PELOTON_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto column_name = reinterpret_cast<expression::TupleValueExpression *>(col)
                           ->GetColFullName();
    std::shared_ptr<ColumnStats> column_stats = std::make_shared<ColumnStats>(
        *memo_->GetGroupByID(gexpr_->GetChildGroupId(0))
             ->GetStats(column_name));
    column_stats->num_rows = group->GetNumRows();
    group->AddStats(column_name, column_stats);
  }
}

void StatsCalculator::Visit(const LogicalDistinct *) {
  // TODO calculate stats for distinct
  memo_->GetGroupByID(gexpr_->GetGroupID())
      ->SetNumRows(
          memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows());
  PELOTON_ASSERT(gexpr_->GetChildrenGroupsSize() == 1);
  for (auto &col : required_cols_) {
    PELOTON_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto column_name = reinterpret_cast<expression::TupleValueExpression *>(col)
                           ->GetColFullName();
    memo_->GetGroupByID(gexpr_->GetGroupID())
        ->AddStats(column_name, memo_->GetGroupByID(gexpr_->GetChildGroupId(0))
                                    ->GetStats(column_name));
  }
}

void StatsCalculator::AddBaseTableStats(
    expression::AbstractExpression *col,
    std::shared_ptr<TableStats> table_stats,
    std::unordered_map<std::string, std::shared_ptr<ColumnStats>> &stats,
    bool copy) {
  PELOTON_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE);
  auto tv_expr = reinterpret_cast<expression::TupleValueExpression *>(col);
  auto bound_ids = tv_expr->GetBoundOid();
  if (table_stats->GetColumnCount() == 0) {
    // We do not have stats for the table yet, use default value
    stats[tv_expr->GetColFullName()] = std::make_shared<ColumnStats>(
        std::get<0>(bound_ids), std::get<1>(bound_ids), std::get<2>(bound_ids),
        tv_expr->GetColFullName(), false, 0, 0.f, false, std::vector<double>{},
        std::vector<double>{}, std::vector<double>{});
  } else {
    stats[tv_expr->GetColFullName()] =
        copy ? std::make_shared<ColumnStats>(
                   *table_stats->GetColumnStats(std::get<2>(bound_ids)))
             : table_stats->GetColumnStats(std::get<2>(bound_ids));
  }
}

void StatsCalculator::UpdateStatsForFilter(
    size_t num_rows,
    std::unordered_map<std::string, std::shared_ptr<ColumnStats>>
        &predicate_stats,
    const std::vector<AnnotatedExpression> &predicates) {
  // First, construct the table stats as the interface needed it to compute
  // selectivity
  // TODO(boweic): We may want to modify the interface of selectivity
  // computation to not use table_stats
  std::vector<std::shared_ptr<ColumnStats>> predicate_stats_vec;
  for (auto &column_name_stats_pair : predicate_stats) {
    predicate_stats_vec.push_back(column_name_stats_pair.second);
  }
  std::shared_ptr<TableStats> predicate_table_stats =
      std::make_shared<TableStats>(predicate_stats_vec);
  double selectivity = 1.f;
  for (auto &annotated_expr : predicates) {
    // Loop over conjunction exprs
    selectivity *= util::CalculateSelectivityForPredicate(
        predicate_table_stats, annotated_expr.expr.get());
  }
  // Update selectivity
  memo_->GetGroupByID(gexpr_->GetGroupID())->SetNumRows(num_rows * selectivity);
}

}  // namespace optimizer
}  // namespace peloton
