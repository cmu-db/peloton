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

  virtual void visit(const PhysicalScan *) = 0;
  virtual void visit(const PhysicalComputeExprs *) = 0;
  virtual void visit(const PhysicalFilter *) = 0;
  virtual void visit(const PhysicalInnerNLJoin *) = 0;
  virtual void visit(const PhysicalLeftNLJoin *) = 0;
  virtual void visit(const PhysicalRightNLJoin *) = 0;
  virtual void visit(const PhysicalOuterNLJoin *) = 0;
  virtual void visit(const PhysicalInnerHashJoin *) = 0;
  virtual void visit(const PhysicalLeftHashJoin *) = 0;
  virtual void visit(const PhysicalRightHashJoin *) = 0;
  virtual void visit(const PhysicalOuterHashJoin *) = 0;
};

} /* namespace optimizer */
} /* namespace peloton */
