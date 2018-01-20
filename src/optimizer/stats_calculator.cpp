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
                                     ExprSet required_cols, Memo *memo) {
  gexpr_ = gexpr;
  memo_ = memo;
  required_cols_ = required_cols;
  gexpr->Op().Accept(this);
}

void StatsCalculator::Visit(const LogicalGet *op) {
  auto table_stats = std::dynamic_pointer_cast<TableStats>(
      StatsStorage::GetInstance()->GetTableStats(op->table->GetDatabaseOid(),
                                                 op->table->GetTableOid()));
  // First, get the required stats of the base table
  std::unordered_map<std::string, std::shared_ptr<ColumnStats>> required_stats;
  for (auto &col : required_cols_) {
    // Make a copy for required stats since we may want to modify later
    AddBaseTableStats(col, table_stats, required_stats, true);
  }
  std::unordered_map<std::string, std::shared_ptr<ColumnStats>> predicate_stats;
  for (auto &annotated_expr : op->predicates) {
    auto predicate = annotated_expr.expr.get();
    ExprSet expr_set;
    expression::ExpressionUtil::GetTupleValueExprs(expr_set, predicate);
    for (auto &col : expr_set) {
      // Do not copy column stats in predicates since we do not need to modify
      AddBaseTableStats(col, table_stats, predicate_stats, false);
    }
  }
  // Next, use predicates to update the stats accordingly
  UpdateStatsForFilter(required_stats, predicate_stats, op->predicates);
  // At last, add the stats to the group
  for (auto &column_name_stats_pair : required_stats) {
    auto &column_name = column_name_stats_pair.first;
    auto &column_stats = column_name_stats_pair.second;
    memo_->GetGroupByID(gexpr_->GetGroupID())
        ->AddStats(column_name, column_stats);
  }
}

void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalQueryDerivedGet *op) {
  // TODO(boweic):
}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalInnerJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalLeftJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalRightJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalOuterJoin *op) {}
void StatsCalculator::Visit(UNUSED_ATTRIBUTE const LogicalSemiJoin *op) {}
void StatsCalculator::Visit(
    UNUSED_ATTRIBUTE const LogicalAggregateAndGroupBy *op) {
  // TODO(boweic): For now we just pass the stats needed without any
  // computation,
  // We need to implement stats computation for aggregation
  PL_ASSERT(gexpr_->GetChildrenGroupsSize() == 1);
  for (auto &col : required_cols_) {
    PL_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE);
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
  PL_ASSERT(expr->GetExpressionType() == ExpressionType::VALUE_TUPLE);
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
    std::unordered_map<std::string, std::shared_ptr<ColumnStats>>
        &required_stats,
    std::unordered_map<std::string, std::shared_ptr<ColumnStats>>
        &predicate_stats,
    std::vector<AnnotatedExpression> &predicates) {
  // First, construct the table stats as the interface needed it to compute
  // selectivity
  // TODO(boweic): Maybe we would want to modify the interface of selectivity
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
    selectivity *= CalculateSelectivityForPredicate(predicate_table_stats,
                                                    annotated_expr.expr.get());
  }
  // Update selectivity
  for (auto &column_name_stats_pair : required_stats) {
    column_name_stats_pair.second->num_rows *= selectivity;
  }
}

// Calculate the selectivity given the predicate and the stats of columns in the
// predicate
double StatsCalculator::CalculateSelectivityForPredicate(
    const std::shared_ptr<TableStats> predicate_table_stats,
    const expression::AbstractExpression *expr) {
  double selectivity = 1.f;
  // Base case : Column Op Val
  if ((expr->GetChild(0)->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
       (expr->GetChild(1)->GetExpressionType() ==
            ExpressionType::VALUE_CONSTANT ||
        expr->GetChild(1)->GetExpressionType() ==
            ExpressionType::VALUE_PARAMETER)) ||
      (expr->GetChild(1)->GetExpressionType() == ExpressionType::VALUE_TUPLE &&
       (expr->GetChild(0)->GetExpressionType() ==
            ExpressionType::VALUE_CONSTANT ||
        expr->GetChild(0)->GetExpressionType() ==
            ExpressionType::VALUE_PARAMETER))) {
    int right_index =
        expr->GetChild(0)->GetExpressionType() == ExpressionType::VALUE_TUPLE
            ? 1
            : 0;

    auto left_expr = expr->GetChild(1 - right_index);
    PL_ASSERT(left_expr->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto col_name =
        reinterpret_cast<const expression::TupleValueExpression *>(left_expr)
            ->GetColFullName();

    auto expr_type = expr->GetExpressionType();
    if (right_index == 0) {
      switch (expr_type) {
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
          expr_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
          break;
        case ExpressionType::COMPARE_LESSTHAN:
          expr_type = ExpressionType::COMPARE_GREATERTHAN;
          break;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          expr_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
          break;
        case ExpressionType::COMPARE_GREATERTHAN:
          expr_type = ExpressionType::COMPARE_LESSTHAN;
          break;
        default:
          break;
      }
    }

    type::Value value;
    if (expr->GetChild(right_index)->GetExpressionType() ==
        ExpressionType::VALUE_CONSTANT) {
      value = reinterpret_cast<expression::ConstantValueExpression *>(
                  expr->GetModifiableChild(right_index))
                  ->GetValue();
    } else {
      value = type::ValueFactory::GetParameterOffsetValue(
                  reinterpret_cast<expression::ParameterValueExpression *>(
                      expr->GetModifiableChild(right_index))
                      ->GetValueIdx())
                  .Copy();
    }
    ValueCondition condition(col_name, expr_type, value);
    selectivity =
        Selectivity::ComputeSelectivity(predicate_table_stats, condition);
  } else if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND ||
             expr->GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
    double left_selectivity = CalculateSelectivityForPredicate(
        predicate_table_stats, expr->GetChild(0));
    double right_selectivity = CalculateSelectivityForPredicate(
        predicate_table_stats, expr->GetChild(1));
    if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
      selectivity = left_selectivity * right_selectivity;
    } else {
      selectivity = left_selectivity + right_selectivity -
                    left_selectivity * right_selectivity;
    }
  }
  return selectivity;
}

}  // namespace optimizer
}  // namespace peloton
