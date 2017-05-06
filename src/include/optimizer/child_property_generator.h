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
class Memo;
}

namespace optimizer {

// Generate child property requirements for physical operators
class ChildPropertyGenerator : public OperatorVisitor {
 public:
  ChildPropertyGenerator(ColumnManager &manager) : manager_(manager) {}

  std::vector<std::pair<PropertySet, std::vector<PropertySet>>> GetProperties(
      std::shared_ptr<GroupExpression> gexpr, PropertySet requirements,
      Memo *memo);

  void Visit(const DummyScan *) override;
  void Visit(const PhysicalSeqScan *) override;
  void Visit(const PhysicalIndexScan *) override;
  void Visit(const PhysicalProject *) override;
  void Visit(const PhysicalOrderBy *) override;
  void Visit(const PhysicalLimit *) override;
  void Visit(const PhysicalFilter *) override;
  void Visit(const PhysicalInnerNLJoin *) override;
  void Visit(const PhysicalLeftNLJoin *) override;
  void Visit(const PhysicalRightNLJoin *) override;
  void Visit(const PhysicalOuterNLJoin *) override;
  void Visit(const PhysicalInnerHashJoin *) override;
  void Visit(const PhysicalLeftHashJoin *) override;
  void Visit(const PhysicalRightHashJoin *) override;
  void Visit(const PhysicalOuterHashJoin *) override;
  void Visit(const PhysicalInsert *) override;
  void Visit(const PhysicalDelete *) override;
  void Visit(const PhysicalUpdate *) override;
  void Visit(const PhysicalHashGroupBy *) override;
  void Visit(const PhysicalSortGroupBy *) override;
  void Visit(const PhysicalDistinct *) override;
  void Visit(const PhysicalAggregate *) override;

 private:
  /***** Helper functions *****/
  // Since the ChildPropertyGenerator have similar
  // behaviour for different implementation of the same
  // logical operator, we apply helper functions to
  // reduce duplicate code
  void AggregateHelper(const BaseOperatorNode *op);
  void ScanHelper();
  void JoinHelper(const BaseOperatorNode *op);

 private:
  ColumnManager &manager_;
  PropertySet requirements_;
  // Each child group contains the base table in that group
  // when deriving column properties for join,
  // we need to assign column to the correct child
  std::vector<Group *> child_groups_;
  std::vector<std::pair<PropertySet, std::vector<PropertySet>>> output_;
};

} /* namespace optimizer */
} /* namespace peloton */
