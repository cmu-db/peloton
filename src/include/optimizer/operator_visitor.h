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

  virtual void Visit(const DummyScan *) = 0;
  virtual void Visit(const PhysicalSeqScan *) = 0;
  virtual void Visit(const PhysicalIndexScan *) = 0;
  virtual void Visit(const PhysicalProject *) = 0;
  virtual void Visit(const PhysicalOrderBy *) = 0;
  virtual void Visit(const PhysicalLimit *) = 0;
  virtual void Visit(const PhysicalFilter *) = 0;
  virtual void Visit(const PhysicalInnerNLJoin *) = 0;
  virtual void Visit(const PhysicalLeftNLJoin *) = 0;
  virtual void Visit(const PhysicalRightNLJoin *) = 0;
  virtual void Visit(const PhysicalOuterNLJoin *) = 0;
  virtual void Visit(const PhysicalInnerHashJoin *) = 0;
  virtual void Visit(const PhysicalLeftHashJoin *) = 0;
  virtual void Visit(const PhysicalRightHashJoin *) = 0;
  virtual void Visit(const PhysicalOuterHashJoin *) = 0;
  virtual void Visit(const PhysicalInsert *) = 0;
  virtual void Visit(const PhysicalDelete *) = 0;
  virtual void Visit(const PhysicalUpdate *) = 0;
  virtual void Visit(const PhysicalHashGroupBy *) = 0;
  virtual void Visit(const PhysicalSortGroupBy *) = 0;
  virtual void Visit(const PhysicalDistinct *) = 0;
  virtual void Visit(const PhysicalAggregate *) = 0;
};

} /* namespace optimizer */
} /* namespace peloton */
