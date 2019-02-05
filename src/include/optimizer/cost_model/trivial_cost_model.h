//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// trivial_cost_calculator.h
//
// Identification: src/include/optimizer/trivial_cost_calculator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "optimizer/cost_model/abstract_cost_model.h"
#include "abstract_cost_model.h"

#include "expression/tuple_value_expression.h"
#include "catalog/table_catalog.h"
#include "optimizer/memo.h"
#include "optimizer/operators.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"

// This cost model is meant to just be a trivial cost model. The decisions it makes are as follows
// * Always choose index scan (cost of 0) over sequential scan (cost of 1)
// * Choose NL if left rows is a single record (for single record lookup queries), else choose hash join
// * Choose hash group by over sort group by

namespace peloton {
namespace optimizer {

class Memo;
class TrivialCostModel : public AbstractCostModel {
 public:
  TrivialCostModel(){};

  double CalculateCost(GroupExpression *gexpr, Memo *memo,
                       concurrency::TransactionContext *txn) override {
    gexpr_ = gexpr;
    memo_ = memo;
    txn_ = txn;
    gexpr_->Op().Accept(this);
    return output_cost_;
  };

  void Visit(UNUSED_ATTRIBUTE const DummyScan *op) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalSeqScan *op) override {
    output_cost_ = 1.f;
  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalIndexScan *op) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const QueryDerivedScan *op) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalOrderBy *) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalLimit *op) override {
    output_cost_ = 0.f;
  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalInnerNLJoin *op) override {
    auto left_child_rows =
        memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetNumRows();
    if (left_child_rows == 1) {
      output_cost_ = 0.f;
    } else {
      output_cost_ = 2.f;
    }
  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalLeftNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalRightNLJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalOuterNLJoin *op) override {}

  void Visit(UNUSED_ATTRIBUTE const PhysicalInnerHashJoin *op) override {
    output_cost_ = 1.f;
  }

  void Visit(UNUSED_ATTRIBUTE const PhysicalLeftHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalRightHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalOuterHashJoin *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalInsert *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalInsertSelect *op) override {}
  void Visit(UNUSED_ATTRIBUTE const PhysicalDelete *op) override{}
  void Visit(UNUSED_ATTRIBUTE const PhysicalUpdate *op) override {}

  void Visit(UNUSED_ATTRIBUTE const PhysicalHashGroupBy *op) override {
    output_cost_ = 0.f;
  }
  void Visit(UNUSED_ATTRIBUTE const PhysicalSortGroupBy *op) override {
    output_cost_ = 1.f;
  }
  void Visit(UNUSED_ATTRIBUTE const PhysicalDistinct *op) override {
    output_cost_ = 0.f;
  }
  void Visit(UNUSED_ATTRIBUTE const PhysicalAggregate *op) override {
    output_cost_ = 0.f;
  }

 private:
  GroupExpression *gexpr_;
  Memo *memo_;
  concurrency::TransactionContext *txn_;
  double output_cost_ = 0;
};

}  // namespace optimizer
}  // namespace peloton