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

#include "optimizer/child_stats_deriver.h"

#include "expression/expression_util.h"
#include "expression/tuple_value_expression.h"
#include "optimizer/memo.h"

namespace peloton {
namespace optimizer {

using std::vector;
vector<ExprSet> ChildStatsDeriver::DeriveInputStats(GroupExpression *gexpr,
                                                    ExprSet required_cols,
                                                    Memo *memo) {
  required_cols_ = required_cols;
  gexpr_ = gexpr;
  memo_ = memo;
  output_ = vector<ExprSet>(gexpr->GetChildrenGroupsSize(), ExprSet{});
  gexpr->Op().Accept(this);
  return std::move(output_);
}

// TODO(boweic): support stats derivation for derivedGet
void ChildStatsDeriver::Visit(const LogicalQueryDerivedGet *) {}
void ChildStatsDeriver::Visit(const LogicalInnerJoin *op) {
  PassDownRequiredCols();
  for (auto &annotated_expr : op->join_predicates) {
    auto predicate = annotated_expr.expr.get();
    ExprSet expr_set;
    expression::ExpressionUtil::GetTupleValueExprs(expr_set, predicate);
    for (auto &col : expr_set) {
      PassDownColumn(col);
    }
  }
}
void ChildStatsDeriver::Visit(UNUSED_ATTRIBUTE const LogicalLeftJoin *) {}
void ChildStatsDeriver::Visit(UNUSED_ATTRIBUTE const LogicalRightJoin *) {}
void ChildStatsDeriver::Visit(UNUSED_ATTRIBUTE const LogicalOuterJoin *) {}
void ChildStatsDeriver::Visit(const LogicalSemiJoin *) {}
// TODO(boweic): support stats of aggregation
void ChildStatsDeriver::Visit(const LogicalAggregateAndGroupBy *) {
  PassDownRequiredCols();
}

void ChildStatsDeriver::PassDownRequiredCols() {
  for (auto &col : required_cols_) {
    // For now we only consider stats of single column
    PassDownColumn(col);
  }
}

void ChildStatsDeriver::PassDownColumn(expression::AbstractExpression *col) {
  PELOTON_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE);
  auto tv_expr = reinterpret_cast<expression::TupleValueExpression *>(col);
  for (size_t idx = 0; idx < gexpr_->GetChildrenGroupsSize(); ++idx) {
    auto child_group = memo_->GetGroupByID(gexpr_->GetChildGroupId(idx));
    if (child_group->GetTableAliases().count(tv_expr->GetTableName()) &&
        // If we have not derived the column stats yet
        child_group->HasColumnStats(tv_expr->GetColFullName())) {
      output_[idx].insert(col);
      break;
    }
  }
}
}  // namespace optimizer
}  // namespace peloton
