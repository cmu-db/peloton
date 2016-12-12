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

  virtual void Visit(const PhysicalScan *) = 0;
  virtual void Visit(const PhysicalProject *) = 0;
  virtual void Visit(const PhysicalComputeExprs *) = 0;
  virtual void Visit(const PhysicalFilter *) = 0;
  virtual void Visit(const PhysicalInnerNLJoin *) = 0;
  virtual void Visit(const PhysicalLeftNLJoin *) = 0;
  virtual void Visit(const PhysicalRightNLJoin *) = 0;
  virtual void Visit(const PhysicalOuterNLJoin *) = 0;
  virtual void Visit(const PhysicalInnerHashJoin *) = 0;
  virtual void Visit(const PhysicalLeftHashJoin *) = 0;
  virtual void Visit(const PhysicalRightHashJoin *) = 0;
  virtual void Visit(const PhysicalOuterHashJoin *) = 0;
};

} /* namespace optimizer */
} /* namespace peloton */
