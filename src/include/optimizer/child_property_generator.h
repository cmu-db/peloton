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

class ChildPropertyGenerator : public OperatorVisitor {
 public:
  ChildPropertyGenerator(ColumnManager &manager) : manager(manager) {}

  std::vector<std::pair<PropertySet, std::vector<PropertySet>>> GetProperties(
      UNUSED_ATTRIBUTE std::shared_ptr<GroupExpression> gexpr,
      UNUSED_ATTRIBUTE PropertySet requirements) {
    return std::move(
        std::vector<std::pair<PropertySet, std::vector<PropertySet>>>());
  }

  void visit(const LeafOperator *) override;
  void visit(const LogicalGet *) override;
  void visit(const LogicalProject *) override;
  void visit(const LogicalFilter *) override;
  void visit(const LogicalInnerJoin *) override;
  void visit(const LogicalLeftJoin *) override;
  void visit(const LogicalRightJoin *) override;
  void visit(const LogicalOuterJoin *) override;
  void visit(const LogicalAggregate *) override;
  void visit(const LogicalLimit *) override;
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
  void visit(const QueryExpressionOperator *) override;
  void visit(const ExprVariable *) override;
  void visit(const ExprConstant *) override;
  void visit(const ExprCompare *) override;
  void visit(const ExprBoolOp *) override;
  void visit(const ExprOp *) override;
  void visit(const ExprProjectList *) override;
  void visit(const ExprProjectColumn *) override;

 private:
  ColumnManager &manager;
};

} /* namespace optimizer */
} /* namespace peloton */
