//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_visitor.cpp
//
// Identification: src/optimizer/operator_visitor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/operator_visitor.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator Visitor
//===--------------------------------------------------------------------===//

void OperatorVisitor::visit(const LeafOperator*) {}

void OperatorVisitor::visit(const LogicalGet*) {}

void OperatorVisitor::visit(const LogicalProject*) {}

void OperatorVisitor::visit(const LogicalSelect*) {}

void OperatorVisitor::visit(const LogicalInnerJoin*) {}

void OperatorVisitor::visit(const LogicalLeftJoin*) {}

void OperatorVisitor::visit(const LogicalRightJoin*) {}

void OperatorVisitor::visit(const LogicalOuterJoin*) {}

void OperatorVisitor::visit(const LogicalAggregate*) {}

void OperatorVisitor::visit(const LogicalLimit*) {}

void OperatorVisitor::visit(const PhysicalScan*) {}

void OperatorVisitor::visit(const PhysicalComputeExprs*) {}

void OperatorVisitor::visit(const PhysicalFilter*) {}

void OperatorVisitor::visit(const PhysicalInnerNLJoin*) {}

void OperatorVisitor::visit(const PhysicalLeftNLJoin*) {}

void OperatorVisitor::visit(const PhysicalRightNLJoin*) {}

void OperatorVisitor::visit(const PhysicalOuterNLJoin*) {}

void OperatorVisitor::visit(const PhysicalInnerHashJoin*) {}

void OperatorVisitor::visit(const PhysicalLeftHashJoin*) {}

void OperatorVisitor::visit(const PhysicalRightHashJoin*) {}

void OperatorVisitor::visit(const PhysicalOuterHashJoin*) {}

void OperatorVisitor::visit(const ExprVariable*) {}

void OperatorVisitor::visit(const ExprConstant*) {}

void OperatorVisitor::visit(const ExprCompare*) {}

void OperatorVisitor::visit(const ExprBoolOp*) {}

void OperatorVisitor::visit(const ExprOp*) {}

void OperatorVisitor::visit(const ExprProjectList*) {}

void OperatorVisitor::visit(const ExprProjectColumn*) {}

} /* namespace optimizer */
} /* namespace peloton */
