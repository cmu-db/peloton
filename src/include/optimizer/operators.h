//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operators.h
//
// Identification: src/optimizer/operators.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_node.h"
#include "optimizer/query_operators.h"
#include "optimizer/group.h"
#include "optimizer/column.h"
#include "optimizer/util.h"

#include <vector>

namespace peloton {
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
  static Operator make(storage::DataTable *table,
                       std::vector<Column *> cols);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  storage::DataTable *table;
  std::vector<Column *> columns;
};

//===--------------------------------------------------------------------===//
// Project
//===--------------------------------------------------------------------===//
class LogicalProject : public OperatorNode<LogicalProject> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
class LogicalSelect : public OperatorNode<LogicalSelect> {
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
  static Operator make(storage::DataTable *table, std::vector<Column *> cols);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  storage::DataTable *table;
  std::vector<Column *> columns;
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

//===--------------------------------------------------------------------===//
// Variable
//===--------------------------------------------------------------------===//
class ExprVariable : public OperatorNode<ExprVariable> {
 public:
  static Operator make(Column *column);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  Column *column;
};

//===--------------------------------------------------------------------===//
// Constant
//===--------------------------------------------------------------------===//
class ExprConstant : public OperatorNode<ExprConstant> {
 public:
  static Operator make(Value value);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  Value value;
};

//===--------------------------------------------------------------------===//
// Compare
//===--------------------------------------------------------------------===//
class ExprCompare : public OperatorNode<ExprCompare> {
 public:
  static Operator make(ExpressionType type);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  ExpressionType expr_type;
};

//===--------------------------------------------------------------------===//
// Boolean Operation
//===--------------------------------------------------------------------===//
enum class BoolOpType {
  Not,
  And,
  Or,
};

class ExprBoolOp : public OperatorNode<ExprBoolOp> {
 public:
  static Operator make(BoolOpType type);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  BoolOpType bool_type;
};

//===--------------------------------------------------------------------===//
// Operation (e.g. +, -, string functions)
//===--------------------------------------------------------------------===//
class ExprOp : public OperatorNode<ExprOp> {
 public:
  static Operator make(ExpressionType type, ValueType return_type);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  ExpressionType expr_type;
  ValueType return_type;
};

//===--------------------------------------------------------------------===//
// ProjectList
//===--------------------------------------------------------------------===//
class ExprProjectList : public OperatorNode<ExprProjectList> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// ProjectColumn
//===--------------------------------------------------------------------===//
class ExprProjectColumn : public OperatorNode<ExprProjectColumn> {
 public:
  static Operator make(Column *column);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  Column *column;
};


} /* namespace optimizer */
} /* namespace peloton */
