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
  ChildPropertyGenerator(ColumnManager &manager) : manager_(manager) {}

  std::vector<std::pair<PropertySet, std::vector<PropertySet>>> GetProperties(
      UNUSED_ATTRIBUTE std::shared_ptr<GroupExpression> gexpr,
      UNUSED_ATTRIBUTE PropertySet requirements) {
    return std::move(
        std::vector<std::pair<PropertySet, std::vector<PropertySet>>>());
  }

  void Visit(const PhysicalScan *) override;
  void Visit(const PhysicalProject *) override;
  void Visit(const PhysicalComputeExprs *) override;
  void Visit(const PhysicalFilter *) override;
  void Visit(const PhysicalInnerNLJoin *) override;
  void Visit(const PhysicalLeftNLJoin *) override;
  void Visit(const PhysicalRightNLJoin *) override;
  void Visit(const PhysicalOuterNLJoin *) override;
  void Visit(const PhysicalInnerHashJoin *) override;
  void Visit(const PhysicalLeftHashJoin *) override;
  void Visit(const PhysicalRightHashJoin *) override;
  void Visit(const PhysicalOuterHashJoin *) override;

 private:
  ColumnManager &manager_;
};

} /* namespace optimizer */
} /* namespace peloton */
