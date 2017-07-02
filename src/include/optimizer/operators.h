
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
                       std::string alias = "", bool update = false);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  storage::DataTable *table;
  std::string table_alias;
  bool is_for_update;
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

  std::shared_ptr<expression::AbstractExpression> join_predicate;
};

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
class LogicalLeftJoin : public OperatorNode<LogicalLeftJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  std::shared_ptr<expression::AbstractExpression> join_predicate;
};

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
class LogicalRightJoin : public OperatorNode<LogicalRightJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  std::shared_ptr<expression::AbstractExpression> join_predicate;
};

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
class LogicalOuterJoin : public OperatorNode<LogicalOuterJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  std::shared_ptr<expression::AbstractExpression> join_predicate;
};

//===--------------------------------------------------------------------===//
// SemiJoin
//===--------------------------------------------------------------------===//
class LogicalSemiJoin : public OperatorNode<LogicalSemiJoin> {
 public:
  static Operator make(expression::AbstractExpression *condition = nullptr);

  std::shared_ptr<expression::AbstractExpression> join_predicate;
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
// DummyScan
//===--------------------------------------------------------------------===//
class DummyScan : public OperatorNode<DummyScan> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// SeqScan
//===--------------------------------------------------------------------===//
class PhysicalSeqScan : public OperatorNode<PhysicalSeqScan> {
 public:
  static Operator make(storage::DataTable *table, std::string alias,
                       bool update);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;
  std::string table_alias;
  bool is_for_update;
  storage::DataTable *table_;
};

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
class PhysicalIndexScan : public OperatorNode<PhysicalIndexScan> {
 public:
  static Operator make(storage::DataTable *table, std::string alias,
                       bool update);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;
  std::string table_alias;
  bool is_for_update;
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
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
class PhysicalLeftNLJoin : public OperatorNode<PhysicalLeftNLJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
class PhysicalRightNLJoin : public OperatorNode<PhysicalRightNLJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
class PhysicalOuterNLJoin : public OperatorNode<PhysicalOuterNLJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
class PhysicalInnerHashJoin : public OperatorNode<PhysicalInnerHashJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
class PhysicalLeftHashJoin : public OperatorNode<PhysicalLeftHashJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
class PhysicalRightHashJoin : public OperatorNode<PhysicalRightHashJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
class PhysicalOuterHashJoin : public OperatorNode<PhysicalOuterHashJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(std::shared_ptr<expression::AbstractExpression> join_predicate);
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

  bool operator==(const BaseOperatorNode &r) override;
  hash_t Hash() const override;

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

  bool operator==(const BaseOperatorNode &r) override;
  hash_t Hash() const override;

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
