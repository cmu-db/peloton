
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

#include <vector>

namespace peloton {

namespace expression {
class AbstractExpression;
}

namespace parser {
class UpdateClause;
}

namespace storage {
class DataTable;
}

namespace optimizer {
class PropertySort;
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
  static Operator make(storage::DataTable *table = nullptr,
                       std::string alias = "");

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  storage::DataTable *table;
  std::string table_alias;
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
  static Operator make(expression::AbstractExpression *condition = nullptr);

  expression::AbstractExpression *condition;
};

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
class LogicalLeftJoin : public OperatorNode<LogicalLeftJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  expression::AbstractExpression *condition;
};

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
class LogicalRightJoin : public OperatorNode<LogicalRightJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  expression::AbstractExpression *condition;
};

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
class LogicalOuterJoin : public OperatorNode<LogicalOuterJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  expression::AbstractExpression *condition;
};

//===--------------------------------------------------------------------===//
// SemiJoin
//===--------------------------------------------------------------------===//
class LogicalSemiJoin : public OperatorNode<LogicalSemiJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  expression::AbstractExpression *condition;
};

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
class LogicalAggregate : public OperatorNode<LogicalAggregate> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// GroupBy
//===--------------------------------------------------------------------===//
class LogicalGroupBy : public OperatorNode<LogicalGroupBy> {
 public:
  static Operator make(
      std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
      expression::AbstractExpression *having);

  std::vector<std::shared_ptr<expression::AbstractExpression>> columns;
  expression::AbstractExpression *having;
};

//===--------------------------------------------------------------------===//
// Limit
//===--------------------------------------------------------------------===//
class LogicalLimit : public OperatorNode<LogicalLimit> {
 public:
  static Operator make(int64_t limit, int64_t offset);

  int64_t limit;
  int64_t offset;
};

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
class LogicalInsert : public OperatorNode<LogicalInsert> {
 public:
  static Operator make(
      storage::DataTable *target_table, std::vector<char *> *columns,
      std::vector<std::vector<peloton::expression::AbstractExpression *> *> *
          values);

  storage::DataTable *target_table;
  std::vector<char *> *columns;
  std::vector<std::vector<peloton::expression::AbstractExpression *> *> *values;
};

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
class LogicalDelete : public OperatorNode<LogicalDelete> {
 public:
  static Operator make(storage::DataTable *target_table);

  storage::DataTable *target_table;
};

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
class LogicalUpdate : public OperatorNode<LogicalUpdate> {
 public:
  static Operator make(storage::DataTable *target_table,
                       std::vector<peloton::parser::UpdateClause*> updates);

  storage::DataTable *target_table;
  std::vector<peloton::parser::UpdateClause*> updates;
};

//===--------------------------------------------------------------------===//
// SeqScan
//===--------------------------------------------------------------------===//
class PhysicalSeqScan : public OperatorNode<PhysicalSeqScan> {
 public:
  static Operator make(storage::DataTable *table);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  storage::DataTable *table_;
};

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
class PhysicalIndexScan : public OperatorNode<PhysicalIndexScan> {
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
// PhysicalOrderBy
//===--------------------------------------------------------------------===//
class PhysicalOrderBy : public OperatorNode<PhysicalOrderBy> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// PhysicalLimit
//===--------------------------------------------------------------------===//
class PhysicalLimit : public OperatorNode<PhysicalLimit> {
 public:
  static Operator make(int64_t limit, int64_t offset);

  int64_t limit;
  int64_t offset;
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
// PhysicalInsert
//===--------------------------------------------------------------------===//
class PhysicalInsert : public OperatorNode<PhysicalInsert> {
 public:
  static Operator make(
      storage::DataTable *target_table, std::vector<char *> *columns,
      std::vector<std::vector<peloton::expression::AbstractExpression *> *> *
          values);

  storage::DataTable *target_table;
  std::vector<char *> *columns;
  std::vector<std::vector<peloton::expression::AbstractExpression *> *> *values;
};

//===--------------------------------------------------------------------===//
// PhysicalDelete
//===--------------------------------------------------------------------===//
class PhysicalDelete : public OperatorNode<PhysicalDelete> {
 public:
  static Operator make(storage::DataTable *target_table);
  storage::DataTable *target_table;
};

//===--------------------------------------------------------------------===//
// PhysicalUpdate
//===--------------------------------------------------------------------===//
class PhysicalUpdate : public OperatorNode<PhysicalUpdate> {
 public:
  static Operator make(storage::DataTable *target_table,
  std::vector<peloton::parser::UpdateClause*> updates);

  storage::DataTable *target_table;
  std::vector<peloton::parser::UpdateClause*> updates;
};

//===--------------------------------------------------------------------===//
// PhysicalHashGroupBy
//===--------------------------------------------------------------------===//
class PhysicalHashGroupBy : public OperatorNode<PhysicalHashGroupBy> {
 public:
  static Operator make(
      std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
      expression::AbstractExpression *having);

  std::vector<std::shared_ptr<expression::AbstractExpression>> columns;
  expression::AbstractExpression *having;
};

//===--------------------------------------------------------------------===//
// PhysicalSortGroupBy
//===--------------------------------------------------------------------===//
class PhysicalSortGroupBy : public OperatorNode<PhysicalSortGroupBy> {
 public:
  static Operator make(
      std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
      expression::AbstractExpression *having);

  std::vector<std::shared_ptr<expression::AbstractExpression>> columns;
  expression::AbstractExpression *having;
};

//===--------------------------------------------------------------------===//
// PhysicalAggregate
//===--------------------------------------------------------------------===//
class PhysicalAggregate : public OperatorNode<PhysicalAggregate> {
 public:
  static Operator make();
};

class PhysicalDistinct : public OperatorNode<PhysicalDistinct> {
 public:
  static Operator make();
};

} /* namespace optimizer */
} /* namespace peloton */
