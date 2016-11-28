//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_query_to_op.h
//
// Identification: src/include/optimizer/child_property_generator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_visitor.h"

namespace peloton {

namespace optimizer {
class ColumnManager;
}

namespace optimizer {

// Generate child property requirements for physical operators
class ChildPropertyGenerator : public OperatorVisitor {
 public:
  ChildPropertyGenerator(ColumnManager &manager) : manager(manager) {}

  std::vector<std::pair<PropertySet, std::vector<PropertySet>>> GetProperties(
      UNUSED_ATTRIBUTE std::shared_ptr<GroupExpression> gexpr,
      UNUSED_ATTRIBUTE PropertySet requirements) {
    return std::move(
        std::vector<std::pair<PropertySet, std::vector<PropertySet>>>());
  }

  void visit(const PhysicalScan *) override;
  void visit(const PhysicalComputeExprs *) override;
  void visit(const PhysicalFilter *) override;
  void visit(const PhysicalInnerNLJoin *) override;
  void visit(const PhysicalLeftNLJoin *) override;
  void visit(const PhysicalRightNLJoin *) override;
  void visit(const PhysicalOuterNLJoin *) override;
  void visit(const PhysicalInnerHashJoin *) override;
  void visit(const PhysicalLeftHashJoin *) override;
  void visit(const PhysicalRightHashJoin *) override;
  void visit(const PhysicalOuterHashJoin *) override;

 private:
  ColumnManager &manager;
};

} /* namespace optimizer */
} /* namespace peloton */
