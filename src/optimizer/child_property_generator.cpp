//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_query_to_op.h
//
// Identification: src/include/optimizer/child_property_generator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/child_property_generator.h"

namespace peloton {
namespace optimizer {

void ChildPropertyGenerator::visit(const LeafOperator *) {}
void ChildPropertyGenerator::visit(const LogicalGet *){};
void ChildPropertyGenerator::visit(const LogicalProject *){};
void ChildPropertyGenerator::visit(const LogicalFilter *){};
void ChildPropertyGenerator::visit(const LogicalInnerJoin *){};
void ChildPropertyGenerator::visit(const LogicalLeftJoin *){};
void ChildPropertyGenerator::visit(const LogicalRightJoin *){};
void ChildPropertyGenerator::visit(const LogicalOuterJoin *){};
void ChildPropertyGenerator::visit(const LogicalAggregate *){};
void ChildPropertyGenerator::visit(const LogicalLimit *){};
void ChildPropertyGenerator::visit(const PhysicalScan *){};
void ChildPropertyGenerator::visit(const PhysicalComputeExprs *){};
void ChildPropertyGenerator::visit(const PhysicalFilter *){};
void ChildPropertyGenerator::visit(const PhysicalInnerNLJoin *){};
void ChildPropertyGenerator::visit(const PhysicalLeftNLJoin *){};
void ChildPropertyGenerator::visit(const PhysicalRightNLJoin *){};
void ChildPropertyGenerator::visit(const PhysicalOuterNLJoin *){};
void ChildPropertyGenerator::visit(const PhysicalInnerHashJoin *){};
void ChildPropertyGenerator::visit(const PhysicalLeftHashJoin *){};
void ChildPropertyGenerator::visit(const PhysicalRightHashJoin *){};
void ChildPropertyGenerator::visit(const PhysicalOuterHashJoin *){};
void ChildPropertyGenerator::visit(const QueryExpressionOperator *){};
void ChildPropertyGenerator::visit(const ExprVariable *){};
void ChildPropertyGenerator::visit(const ExprConstant *){};
void ChildPropertyGenerator::visit(const ExprCompare *){};
void ChildPropertyGenerator::visit(const ExprBoolOp *){};
void ChildPropertyGenerator::visit(const ExprOp *){};
void ChildPropertyGenerator::visit(const ExprProjectList *){};
void ChildPropertyGenerator::visit(const ExprProjectColumn *){};

} /* namespace optimizer */
} /* namespace peloton */
