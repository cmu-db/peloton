//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/optimizer/cost_calculator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/cost_calculator.h"

#include <cmath>

#include "catalog/table_catalog.h"
#include "optimizer/memo.h"
#include "optimizer/operators.h"
#include "optimizer/stats/cost.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"

namespace peloton {
namespace optimizer {

double CostCalculator::CalculateCost(GroupExpression *gexpr, Memo *memo,
                                     concurrency::TransactionContext *txn) {
  gexpr_ = gexpr;
  memo_ = memo;
  txn_ = txn;
  gexpr_->Op().Accept(this);
  return output_cost_;
}

void CostCalculator::Visit(UNUSED_ATTRIBUTE const DummyScan *op) {
  output_cost_ = 0.f;
}
void CostCalculator::Visit(const PhysicalSeqScan *op) {
  auto table_stats = std::dynamic_pointer_cast<TableStats>(
      StatsStorage::GetInstance()->GetTableStats(
          op->table_->GetDatabaseOid(), op->table_->GetTableOid(), txn_));
  if (table_stats->GetColumnCount() == 0) {
    output_cost_ = 1.f;
    return;
  }
  output_cost_ = table_stats->num_rows * DEFAULT_TUPLE_COST;
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalIndexScan *op) {
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
void CostCalculator::Visit(UNUSED_ATTRIBUTE const QueryDerivedScan *op) {
  output_cost_ = 0.f;
}

void CostCalculator::Visit(const PhysicalOrderBy *) { SortCost(); }

void CostCalculator::Visit(const PhysicalLimit *op) {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

  output_cost_ =
      std::min((size_t)child_num_rows, (size_t)op->limit) * DEFAULT_TUPLE_COST;
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalInnerNLJoin *op) {
  auto left_child_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
  auto right_child_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();

  output_cost_ = left_child_rows * right_child_rows * DEFAULT_TUPLE_COST;
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalLeftNLJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalRightNLJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalOuterNLJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalInnerHashJoin *op) {
  auto left_child_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
  auto right_child_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetNumRows();
  // TODO(boweic): Build (left) table should have different cost to probe table
  output_cost_ = (left_child_rows + right_child_rows) * DEFAULT_TUPLE_COST;
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalLeftHashJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalRightHashJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalOuterHashJoin *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalInsert *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalInsertSelect *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalDelete *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalUpdate *op) {}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalHashGroupBy *op) {
  // TODO(boweic): Integrate hash in groupby may cause us to miss the
  // opportunity to further optimize some query where the child output is
  // already hashed by the GroupBy key, we'll do a hash anyway
  output_cost_ = HashCost() + GroupByCost();
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalSortGroupBy *op) {
  // Sort group by does not sort the tuples, it requires input columns to be
  // sorted
  output_cost_ = GroupByCost();
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalDistinct *op) {
  output_cost_ = HashCost();
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalAggregate *op) {
  // TODO(boweic): Ditto, separate groupby operator and implementation(e.g.
  // hash, sort) may enable opportunity for further optimization
  output_cost_ = HashCost() + GroupByCost();
}

double CostCalculator::HashCost() {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
  // O(tuple)
  return child_num_rows * DEFAULT_TUPLE_COST;
}

double CostCalculator::SortCost() {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

  if (child_num_rows == 0) {
    return 1.0f;
  }
  // O(tuple * log(tuple))
  return child_num_rows * std::log2(child_num_rows) * DEFAULT_TUPLE_COST;
}

double CostCalculator::GroupByCost() {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
  // O(tuple)
  return child_num_rows * DEFAULT_TUPLE_COST;
}
}  // namespace optimizer
}  // namespace peloton
