//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_cost_calculator.h
//
// Identification: src/include/optimizer/postgres_cost_calculator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "optimizer/abstract_cost_calculator.h"

// TODO: This is not fully reflective of the postgres cost model. Currently we
// are attempting
// to emulate their hash join cost model

namespace peloton {
namespace optimizer {

class Memo;
// Derive cost for a physical group expression
class PostgresCostCalculator : public AbstractCostCalculator {
 public:
  double CalculateCost(GroupExpression *gexpr, Memo *memo,
                       concurrency::TransactionContext *txn) override;

  void Visit(const DummyScan *) override;
  void Visit(const PhysicalSeqScan *) override;
  void Visit(const PhysicalIndexScan *) override;
  void Visit(const QueryDerivedScan *) override;
  void Visit(const PhysicalOrderBy *) override;
  void Visit(const PhysicalLimit *) override;
  void Visit(const PhysicalInnerNLJoin *) override;
  void Visit(const PhysicalLeftNLJoin *) override;
  void Visit(const PhysicalRightNLJoin *) override;
  void Visit(const PhysicalOuterNLJoin *) override;
  void Visit(const PhysicalInnerHashJoin *) override;
  void Visit(const PhysicalLeftHashJoin *) override;
  void Visit(const PhysicalRightHashJoin *) override;
  void Visit(const PhysicalOuterHashJoin *) override;
  void Visit(const PhysicalInsert *) override;
  void Visit(const PhysicalInsertSelect *) override;
  void Visit(const PhysicalDelete *) override;
  void Visit(const PhysicalUpdate *) override;
  void Visit(const PhysicalHashGroupBy *) override;
  void Visit(const PhysicalSortGroupBy *) override;
  void Visit(const PhysicalDistinct *) override;
  void Visit(const PhysicalAggregate *) override;

 private:
  double HashCost();
  double SortCost();
  double GroupByCost();

  /* Checks if keys for a join child only reference one table */
  bool IsBaseTable(
      const std::vector<std::unique_ptr<expression::AbstractExpression>> &keys);

  GroupExpression *gexpr_;
  Memo *memo_;
  concurrency::TransactionContext *txn_;
  double output_cost_ = 0;
};

}  // namespace optimizer
}  // namespace peloton