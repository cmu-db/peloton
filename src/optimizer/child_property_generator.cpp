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

#include "optimizer/child_property_generator.h"
#include "optimizer/column_manager.h"

namespace peloton {
namespace optimizer {

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

} /* namespace optimizer */
} /* namespace peloton */
