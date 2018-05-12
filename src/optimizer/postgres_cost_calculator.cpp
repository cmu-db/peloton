//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_cost_calculator.cpp
//
// Identification: src/optimizer/cost_calculator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/postgres_cost_calculator.h"

#include "expression/tuple_value_expression.h"
#include "catalog/table_catalog.h"
#include "optimizer/memo.h"
#include "optimizer/operators.h"
#include "optimizer/stats/cost.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"

#include <cmath>

namespace peloton {
namespace optimizer {

double PostgresCostCalculator::CalculateCost(
    GroupExpression *gexpr, Memo *memo, concurrency::TransactionContext *txn) {
  gexpr_ = gexpr;
  memo_ = memo;
  txn_ = txn;
  gexpr_->Op().Accept(this);

  return output_cost_;
}

void PostgresCostCalculator::Visit(UNUSED_ATTRIBUTE const DummyScan *op) {
  output_cost_ = 0.f;
}
void PostgresCostCalculator::Visit(const PhysicalSeqScan *op) {
  auto table_stats = std::dynamic_pointer_cast<TableStats>(
      StatsStorage::GetInstance()->GetTableStats(
          op->table_->GetDatabaseOid(), op->table_->GetTableOid(), txn_));
  if (table_stats->GetColumnCount() == 0) {
    output_cost_ = 1.f;
    return;
  }
  output_cost_ = table_stats->num_rows * DEFAULT_TUPLE_COST;
}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalIndexScan *op) {
  auto table_stats = std::dynamic_pointer_cast<TableStats>(
      StatsStorage::GetInstance()->GetTableStats(
          op->table_->GetDatabaseOid(), op->table_->GetTableOid(), txn_));
  if (table_stats->GetColumnCount() == 0 || table_stats->num_rows == 0) {
    output_cost_ = 0.f;
    return;
  }
  // Index search cost + scan cost
  output_cost_ = std::log2(table_stats->num_rows) * DEFAULT_INDEX_TUPLE_COST +
                 memo_->GetGroupByID(gexpr_->GetGroupID())->GetNumRows() *
                     DEFAULT_TUPLE_COST;
}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const QueryDerivedScan *op) {
  output_cost_ = 0.f;
}

void PostgresCostCalculator::Visit(const PhysicalOrderBy *) { SortCost(); }

void PostgresCostCalculator::Visit(const PhysicalLimit *op) {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

  output_cost_ =
      std::min((size_t)child_num_rows, (size_t)op->limit) * DEFAULT_TUPLE_COST;
}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalInnerNLJoin *op) {
  auto left_child_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
  auto right_child_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();

  output_cost_ = left_child_rows * right_child_rows * DEFAULT_TUPLE_COST;
}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalLeftNLJoin *op) {}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalRightNLJoin *op) {}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalOuterNLJoin *op) {}

/* The main idea of this cost estimate is that the comparisons done is the outer
 * table (probe side) times tuples
 * bucket. Thus, we estimate attempt to estimate the bucket size in a similar
 * manner to postgres.
 */
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalInnerHashJoin *op) {
  auto bucket_size_frac = 1.0;

  // Assuming you build table on right relation
  if (IsBaseTable(op->right_keys)) {
    auto right_group = memo_->GetGroupByID(gexpr_->GetChildGroupId(1));

    // Iterate over all keys, take the largest fraction (smallest bucket sizes)
    // TODO: Add more estimate adjustments from postgres
    for (auto &expr : op->right_keys) {
      auto tv_expr =
          reinterpret_cast<expression::TupleValueExpression *>(expr.get());
      auto stats = right_group->GetStats(tv_expr->GetColFullName());

      if (stats == nullptr) continue;

      // TODO: A new hash join PR uses 256 as default so we're using this for
      // now and hardcoding it here
      auto num_buckets = 256.0;

      double frac_est;

      if (stats->cardinality > num_buckets) {
        frac_est = 1.0 / num_buckets;
      } else {
        frac_est = 1.0 / stats->cardinality;
      }

      /* Average frequency of values, taken from Postgres */
      auto avgfreq = (1.0 - stats->frac_null) / stats->cardinality;

      // Adjust for skew
      if (avgfreq > 0.0 && !stats->most_common_vals.empty() &&
          !stats->most_common_freqs.empty() &&
          stats->most_common_freqs[0] > avgfreq) {
        frac_est *= stats->most_common_freqs[0] / avgfreq;
      }

      // Clamp the bucket frac estimate (taken from postgres)
      if (frac_est < 1.0e-6) {
        frac_est = 1.0e-6;
      } else if (frac_est > 1.0) {
        frac_est = 1.0;
      }
      bucket_size_frac = std::min(bucket_size_frac, frac_est);
    }
  }

  auto left_child_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
  auto right_child_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();
  output_cost_ = (left_child_rows * (right_child_rows * bucket_size_frac)) *
                 DEFAULT_TUPLE_COST;
}

void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalLeftHashJoin *op) {}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalRightHashJoin *op) {}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalOuterHashJoin *op) {}
void PostgresCostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalInsert *op) {}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalInsertSelect *op) {}
void PostgresCostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalDelete *op) {}
void PostgresCostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalUpdate *op) {}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalHashGroupBy *op) {
  // TODO(boweic): Integrate hash in groupby may cause us to miss the
  // opportunity to further optimize some query where the child output is
  // already hashed by the GroupBy key, we'll do a hash anyway
  output_cost_ = HashCost() + GroupByCost();
}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalSortGroupBy *op) {
  // Sort group by does not sort the tuples, it requires input columns to be
  // sorted
  output_cost_ = GroupByCost();
}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalDistinct *op) {
  output_cost_ = HashCost();
}
void PostgresCostCalculator::Visit(
    UNUSED_ATTRIBUTE const PhysicalAggregate *op) {
  // TODO(boweic): Ditto, separate groupby operator and implementation(e.g.
  // hash, sort) may enable opportunity for further optimization
  output_cost_ = HashCost() + GroupByCost();
}

double PostgresCostCalculator::HashCost() {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
  // O(tuple)
  return child_num_rows * DEFAULT_TUPLE_COST;
}

double PostgresCostCalculator::SortCost() {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

  if (child_num_rows == 0) {
    return 1.0f;
  }
  // O(tuple * log(tuple))
  return child_num_rows * std::log2(child_num_rows) * DEFAULT_TUPLE_COST;
}

double PostgresCostCalculator::GroupByCost() {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
  // O(tuple)
  return child_num_rows * DEFAULT_TUPLE_COST;
}

// Might be better to implement this using groups as opposed to table names
bool PostgresCostCalculator::IsBaseTable(
    const std::vector<std::unique_ptr<expression::AbstractExpression>> &keys) {
  std::unordered_set<std::string> seen_set;

  for (auto &expr : keys) {
    if (expr->GetExpressionType() != ExpressionType::VALUE_TUPLE) continue;

    auto tv_expr =
        reinterpret_cast<expression::TupleValueExpression *>(expr.get());
    seen_set.insert(tv_expr->GetTableName());
  }

  return seen_set.size() == 1;
}

}  // namespace optimizer
}  // namespace peloton
