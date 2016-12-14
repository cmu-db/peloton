
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operators.h
//
// Identification: src/include/optimizer/operators.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/column.h"
#include "optimizer/group.h"
#include "optimizer/operator_node.h"
#include "optimizer/util.h"

#include <vector>

namespace peloton {

namespace expression {
class AbstractExpression;
}

namespace storage {
class DataTable;
}

namespace optimizer {

//===--------------------------------------------------------------------===//
// Leaf
//===--------------------------------------------------------------------===//
class LeafOperator : OperatorNode<LeafOperator> {
 public:
  static Operator make(GroupID group);

  GroupID origin_group;
};

//===--------------------------------------------------------------------===//
// Get
//===--------------------------------------------------------------------===//
class LogicalGet : public OperatorNode<LogicalGet> {
 public:
  static Operator make(storage::DataTable *table);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  storage::DataTable *table;
};

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
class LogicalFilter : public OperatorNode<LogicalFilter> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
class LogicalInnerJoin : public OperatorNode<LogicalInnerJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
class LogicalLeftJoin : public OperatorNode<LogicalLeftJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
class LogicalRightJoin : public OperatorNode<LogicalRightJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
class LogicalOuterJoin : public OperatorNode<LogicalOuterJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
class LogicalAggregate : public OperatorNode<LogicalAggregate> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// Limit
//===--------------------------------------------------------------------===//
class LogicalLimit : public OperatorNode<LogicalLimit> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
class PhysicalScan : public OperatorNode<PhysicalScan> {
 public:
  static Operator make(storage::DataTable *table);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  storage::DataTable *table_;
};

//===--------------------------------------------------------------------===//
// PhysicalProject
//===--------------------------------------------------------------------===//
class PhysicalProject : public OperatorNode<PhysicalProject> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// ComputeExprs
//===--------------------------------------------------------------------===//
class PhysicalComputeExprs : public OperatorNode<PhysicalComputeExprs> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
class PhysicalFilter : public OperatorNode<PhysicalFilter> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// InnerNLJoin
//===--------------------------------------------------------------------===//
class PhysicalInnerNLJoin : public OperatorNode<PhysicalInnerNLJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
class PhysicalLeftNLJoin : public OperatorNode<PhysicalLeftNLJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
class PhysicalRightNLJoin : public OperatorNode<PhysicalRightNLJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
class PhysicalOuterNLJoin : public OperatorNode<PhysicalOuterNLJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
class PhysicalInnerHashJoin : public OperatorNode<PhysicalInnerHashJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
class PhysicalLeftHashJoin : public OperatorNode<PhysicalLeftHashJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
class PhysicalRightHashJoin : public OperatorNode<PhysicalRightHashJoin> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
class PhysicalOuterHashJoin : public OperatorNode<PhysicalOuterHashJoin> {
 public:
  static Operator make();
};

} /* namespace optimizer */
} /* namespace peloton */
