//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_visitor.h
//
// Identification: src/include/optimizer/operator_visitor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operators.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator Visitor
//===--------------------------------------------------------------------===//

class OperatorVisitor {
 public:
  virtual ~OperatorVisitor(){};

  // Physical operator
  virtual void Visit(const DummyScan *) {}
  virtual void Visit(const PhysicalSeqScan *) {}
  virtual void Visit(const PhysicalIndexScan *) {}
  virtual void Visit(const QueryDerivedScan *) {}
  virtual void Visit(const PhysicalOrderBy *) {}
  virtual void Visit(const PhysicalLimit *) {}
  virtual void Visit(const PhysicalInnerNLJoin *) {}
  virtual void Visit(const PhysicalLeftNLJoin *) {}
  virtual void Visit(const PhysicalRightNLJoin *) {}
  virtual void Visit(const PhysicalOuterNLJoin *) {}
  virtual void Visit(const PhysicalInnerHashJoin *) {}
  virtual void Visit(const PhysicalLeftHashJoin *) {}
  virtual void Visit(const PhysicalRightHashJoin *) {}
  virtual void Visit(const PhysicalOuterHashJoin *) {}
  virtual void Visit(const PhysicalInsert *) {}
  virtual void Visit(const PhysicalInsertSelect *) {}
  virtual void Visit(const PhysicalDelete *) {}
  virtual void Visit(const PhysicalUpdate *) {}
  virtual void Visit(const PhysicalHashGroupBy *) {}
  virtual void Visit(const PhysicalSortGroupBy *) {}
  virtual void Visit(const PhysicalDistinct *) {}
  virtual void Visit(const PhysicalAggregate *) {}

  // Logical operator
  virtual void Visit(const LeafOperator *) {}
  virtual void Visit(const LogicalGet *) {}
  virtual void Visit(const LogicalQueryDerivedGet *) {}
  virtual void Visit(const LogicalFilter *) {}
  virtual void Visit(const LogicalProjection *) {}
  virtual void Visit(const LogicalMarkJoin *) {}
  virtual void Visit(const LogicalSingleJoin *) {}
  virtual void Visit(const LogicalDependentJoin *) {}
  virtual void Visit(const LogicalInnerJoin *) {}
  virtual void Visit(const LogicalLeftJoin *) {}
  virtual void Visit(const LogicalRightJoin *) {}
  virtual void Visit(const LogicalOuterJoin *) {}
  virtual void Visit(const LogicalSemiJoin *) {}
  virtual void Visit(const LogicalAggregateAndGroupBy *) {}
  virtual void Visit(const LogicalInsert *) {}
  virtual void Visit(const LogicalInsertSelect *) {}
  virtual void Visit(const LogicalDelete *) {}
  virtual void Visit(const LogicalUpdate *) {}
  virtual void Visit(const LogicalDistinct *) {}
  virtual void Visit(const LogicalLimit *) {}
};

}  // namespace optimizer
}  // namespace peloton
