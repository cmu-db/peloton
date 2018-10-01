//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_calculator.h
//
// Identification: src/include/optimizer/cost_calculator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/abstract_cost_calculator.h"

namespace peloton {
namespace optimizer {

class Memo;
// Derive cost for a physical group expression
class CostCalculator : public AbstractCostCalculator {
 public:
  CostCalculator(){};

  virtual double CalculateCost(GroupExpression *gexpr, Memo *memo,
                               concurrency::TransactionContext *txn) override;

  virtual void Visit(const DummyScan *) override;
  virtual void Visit(const PhysicalSeqScan *) override;
  virtual void Visit(const PhysicalIndexScan *) override;
  virtual void Visit(const QueryDerivedScan *) override;
  virtual void Visit(const PhysicalOrderBy *) override;
  virtual void Visit(const PhysicalLimit *) override;
  virtual void Visit(const PhysicalInnerNLJoin *) override;
  virtual void Visit(const PhysicalLeftNLJoin *) override;
  virtual void Visit(const PhysicalRightNLJoin *) override;
  virtual void Visit(const PhysicalOuterNLJoin *) override;
  virtual void Visit(const PhysicalInnerHashJoin *) override;
  virtual void Visit(const PhysicalLeftHashJoin *) override;
  virtual void Visit(const PhysicalRightHashJoin *) override;
  virtual void Visit(const PhysicalOuterHashJoin *) override;
  virtual void Visit(const PhysicalInsert *) override;
  virtual void Visit(const PhysicalInsertSelect *) override;
  virtual void Visit(const PhysicalDelete *) override;
  virtual void Visit(const PhysicalUpdate *) override;
  virtual void Visit(const PhysicalHashGroupBy *) override;
  virtual void Visit(const PhysicalSortGroupBy *) override;
  virtual void Visit(const PhysicalDistinct *) override;
  virtual void Visit(const PhysicalAggregate *) override;

 private:
  double HashCost();
  double SortCost();
  double GroupByCost();

  GroupExpression *gexpr_;
  Memo *memo_;
  concurrency::TransactionContext *txn_;
  double output_cost_ = 0;
};

}  // namespace optimizer
}  // namespace peloton
