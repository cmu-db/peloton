//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_visitor.h
//
// Identification: src/optimizer/operator_visitor.h
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
  virtual ~OperatorVisitor() {};

  virtual void visit(const LeafOperator *);
  virtual void visit(const LogicalGet *);
  virtual void visit(const LogicalProject *);
  virtual void visit(const LogicalSelect *);
  virtual void visit(const LogicalInnerJoin *);
  virtual void visit(const LogicalLeftJoin *);
  virtual void visit(const LogicalRightJoin *);
  virtual void visit(const LogicalOuterJoin *);
  virtual void visit(const LogicalAggregate *);
  virtual void visit(const LogicalLimit *);
  virtual void visit(const PhysicalScan *);
  virtual void visit(const PhysicalComputeExprs *);
  virtual void visit(const PhysicalFilter *);
  virtual void visit(const PhysicalInnerNLJoin *);
  virtual void visit(const PhysicalLeftNLJoin *);
  virtual void visit(const PhysicalRightNLJoin *);
  virtual void visit(const PhysicalOuterNLJoin *);
  virtual void visit(const PhysicalInnerHashJoin *);
  virtual void visit(const PhysicalLeftHashJoin *);
  virtual void visit(const PhysicalRightHashJoin *);
  virtual void visit(const PhysicalOuterHashJoin *);
  virtual void visit(const ExprVariable *);
  virtual void visit(const ExprConstant *);
  virtual void visit(const ExprCompare *);
  virtual void visit(const ExprBoolOp *);
  virtual void visit(const ExprOp *);
  virtual void visit(const ExprProjectList *);
  virtual void visit(const ExprProjectColumn *);
};

} /* namespace optimizer */
} /* namespace peloton */
