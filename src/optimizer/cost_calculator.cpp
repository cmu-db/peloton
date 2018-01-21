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

double CostCalculator::CalculateCost(GroupExpression *gexpr, Memo *memo) {
  gexpr_ = gexpr;
  memo_ = memo;
  gexpr_->Op().Accept(this);
  return output_cost_;
}

void CostCalculator::Visit(UNUSED_ATTRIBUTE const DummyScan *op) {
  output_cost_ = 0.f;
}
void CostCalculator::Visit(const PhysicalSeqScan *op) {
  auto table_stats = std::dynamic_pointer_cast<TableStats>(
      StatsStorage::GetInstance()->GetTableStats(op->table_->GetDatabaseOid(),
                                                 op->table_->GetTableOid()));
  if (table_stats->GetColumnCount() == 0) {
    output_cost_ = 1.f;
    return;
  }
  output_cost_ = table_stats->num_rows * DEFAULT_TUPLE_COST;
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalIndexScan *op) {
  auto table_stats = std::dynamic_pointer_cast<TableStats>(
      StatsStorage::GetInstance()->GetTableStats(op->table_->GetDatabaseOid(),
                                                 op->table_->GetTableOid()));
  if (table_stats->GetColumnCount() == 0 || table_stats->num_rows == 0) {
    output_cost_ = 0.f;
    return;
  }
  // Index search cost
  output_cost_ = std::log2(table_stats->num_rows) * DEFAULT_INDEX_TUPLE_COST;
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
  HashCost();
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalSortGroupBy *op) {
  SortCost();
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalDistinct *op) {
  HashCost();
}
void CostCalculator::Visit(UNUSED_ATTRIBUTE const PhysicalAggregate *op) {
  HashCost();
}

void CostCalculator::HashCost() {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
  // Hash Cost
  output_cost_ = child_num_rows * DEFAULT_TUPLE_COST;
}

void CostCalculator::SortCost() {
  auto child_num_rows =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();

  if (child_num_rows == 0) {
    output_cost_ = 1.0f;
    return;
  }
  // sort cost
  output_cost_ =
      child_num_rows * std::log2(child_num_rows) * DEFAULT_TUPLE_COST;
}
}  // namespace optimizer
}  // namespace peloton
