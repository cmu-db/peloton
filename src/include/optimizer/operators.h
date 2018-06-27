//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operators.h
//
// Identification: src/include/optimizer/operators.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.h"
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

namespace catalog {
class TableCatalogEntry;
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
  static Operator make(
      oid_t get_id = 0, std::vector<AnnotatedExpression> predicates = {},
      std::shared_ptr<catalog::TableCatalogEntry> table = nullptr,
      std::string alias = "", bool update = false);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  // identifier for all get operators
  oid_t get_id;
  std::vector<AnnotatedExpression> predicates;
  std::shared_ptr<catalog::TableCatalogEntry> table;
  std::string table_alias;
  bool is_for_update;
};

//===--------------------------------------------------------------------===//
// External file get
//===--------------------------------------------------------------------===//
class LogicalExternalFileGet : public OperatorNode<LogicalExternalFileGet> {
 public:
  static Operator make(oid_t get_id, ExternalFileFormat format,
                       std::string file_name, char delimiter, char quote,
                       char escape);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  // identifier for all get operators
  oid_t get_id;
  ExternalFileFormat format;
  std::string file_name;
  char delimiter;
  char quote;
  char escape;
};

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
class LogicalQueryDerivedGet : public OperatorNode<LogicalQueryDerivedGet> {
 public:
  static Operator make(
      oid_t get_id, std::string &alias,
      std::unordered_map<std::string,
                         std::shared_ptr<expression::AbstractExpression>>
          alias_to_expr_map);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  // identifier for all get operators
  oid_t get_id;
  std::string table_alias;
  std::unordered_map<std::string,
                     std::shared_ptr<expression::AbstractExpression>>
      alias_to_expr_map;
};

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
class LogicalFilter : public OperatorNode<LogicalFilter> {
 public:
  static Operator make(std::vector<AnnotatedExpression> &filter);
  std::vector<AnnotatedExpression> predicates;

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;
};

//===--------------------------------------------------------------------===//
// Project
//===--------------------------------------------------------------------===//
class LogicalProjection : public OperatorNode<LogicalProjection> {
 public:
  static Operator make(
      std::vector<std::shared_ptr<expression::AbstractExpression>> &elements);
  std::vector<std::shared_ptr<expression::AbstractExpression>> expressions;
};

//===--------------------------------------------------------------------===//
// DependentJoin
//===--------------------------------------------------------------------===//
class LogicalDependentJoin : public OperatorNode<LogicalDependentJoin> {
 public:
  static Operator make();

  static Operator make(std::vector<AnnotatedExpression> &conditions);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// MarkJoin
//===--------------------------------------------------------------------===//
class LogicalMarkJoin : public OperatorNode<LogicalMarkJoin> {
 public:
  static Operator make();

  static Operator make(std::vector<AnnotatedExpression> &conditions);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// SingleJoin
//===--------------------------------------------------------------------===//
class LogicalSingleJoin : public OperatorNode<LogicalSingleJoin> {
 public:
  static Operator make();

  static Operator make(std::vector<AnnotatedExpression> &conditions);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
class LogicalInnerJoin : public OperatorNode<LogicalInnerJoin> {
 public:
  static Operator make();

  static Operator make(std::vector<AnnotatedExpression> &conditions);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<AnnotatedExpression> join_predicates;
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
// GroupBy
//===--------------------------------------------------------------------===//
class LogicalAggregateAndGroupBy
    : public OperatorNode<LogicalAggregateAndGroupBy> {
 public:
  static Operator make();

  static Operator make(
      std::vector<std::shared_ptr<expression::AbstractExpression>> &columns);

  static Operator make(
      std::vector<std::shared_ptr<expression::AbstractExpression>> &columns,
      std::vector<AnnotatedExpression> &having);

  bool operator==(const BaseOperatorNode &r) override;
  hash_t Hash() const override;

  std::vector<std::shared_ptr<expression::AbstractExpression>> columns;
  std::vector<AnnotatedExpression> having;
};

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
class LogicalInsert : public OperatorNode<LogicalInsert> {
 public:
  static Operator make(
      std::shared_ptr<catalog::TableCatalogEntry> target_table,
      const std::vector<std::string> *columns,
      const std::vector<std::vector<
          std::unique_ptr<expression::AbstractExpression>>> *values);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  const std::vector<std::string> *columns;
  const std::vector<
      std::vector<std::unique_ptr<expression::AbstractExpression>>> *values;
};

class LogicalInsertSelect : public OperatorNode<LogicalInsertSelect> {
 public:
  static Operator make(
      std::shared_ptr<catalog::TableCatalogEntry> target_table);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
};

//===--------------------------------------------------------------------===//
// LogicalDistinct
//===--------------------------------------------------------------------===//
class LogicalDistinct : public OperatorNode<LogicalDistinct> {
 public:
  static Operator make();
};

//===--------------------------------------------------------------------===//
// LogicalLimit
//===--------------------------------------------------------------------===//
class LogicalLimit : public OperatorNode<LogicalLimit> {
 public:
  static Operator make(
      int64_t offset, int64_t limit,
      std::vector<expression::AbstractExpression *> &&sort_exprs,
      std::vector<bool> &&sort_ascending);
  int64_t offset;
  int64_t limit;
  // When we get a query like "SELECT * FROM tab ORDER BY a LIMIT 5"
  // We'll let the limit operator keep the order by clause's content as an
  // internal order, then the limit operator will generate sort plan with 
  // limit as a optimization.
  std::vector<expression::AbstractExpression *> sort_exprs;
  std::vector<bool> sort_ascending;
};

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
class LogicalDelete : public OperatorNode<LogicalDelete> {
 public:
  static Operator make(
      std::shared_ptr<catalog::TableCatalogEntry> target_table);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
};

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
class LogicalUpdate : public OperatorNode<LogicalUpdate> {
 public:
  static Operator make(
      std::shared_ptr<catalog::TableCatalogEntry> target_table,
      const std::vector<std::unique_ptr<parser::UpdateClause>> *updates);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  const std::vector<std::unique_ptr<parser::UpdateClause>> *updates;
};

//===--------------------------------------------------------------------===//
// Export to external file
//===--------------------------------------------------------------------===//
class LogicalExportExternalFile
    : public OperatorNode<LogicalExportExternalFile> {
 public:
  static Operator make(ExternalFileFormat format, std::string file_name,
                       char delimiter, char quote, char escape);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  ExternalFileFormat format;
  std::string file_name;
  char delimiter;
  char quote;
  char escape;
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
  static Operator make(oid_t get_id,
                       std::shared_ptr<catalog::TableCatalogEntry> table,
                       std::string alias,
                       std::vector<AnnotatedExpression> predicates,
                       bool update);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  // identifier for all get operators
  oid_t get_id;
  std::vector<AnnotatedExpression> predicates;
  std::string table_alias;
  bool is_for_update;
  std::shared_ptr<catalog::TableCatalogEntry> table_;
};

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
class PhysicalIndexScan : public OperatorNode<PhysicalIndexScan> {
 public:
  static Operator make(oid_t get_id,
                       std::shared_ptr<catalog::TableCatalogEntry> table,
                       std::string alias,
                       std::vector<AnnotatedExpression> predicates, bool update,
                       oid_t index_id, std::vector<oid_t> key_column_id_list,
                       std::vector<ExpressionType> expr_type_list,
                       std::vector<type::Value> value_list);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  // identifier for all get operators
  oid_t get_id;
  std::vector<AnnotatedExpression> predicates;
  std::string table_alias;
  bool is_for_update;
  std::shared_ptr<catalog::TableCatalogEntry> table_;

  // Index info.
  // Match planner::IndexScanPlan::IndexScanDesc index_scan_desc(
  //      index, key_column_ids, expr_types, values, runtime_keys)
  oid_t index_id = -1;
  std::vector<oid_t> key_column_id_list;
  std::vector<ExpressionType> expr_type_list;
  std::vector<type::Value> value_list;
};

//===--------------------------------------------------------------------===//
// Physical external file scan
//===--------------------------------------------------------------------===//
class ExternalFileScan : public OperatorNode<ExternalFileScan> {
 public:
  static Operator make(oid_t get_id, ExternalFileFormat format,
                       std::string file_name, char delimiter, char quote,
                       char escape);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  // identifier for all get operators
  oid_t get_id;
  ExternalFileFormat format;
  std::string file_name;
  char delimiter;
  char quote;
  char escape;
};

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
class QueryDerivedScan : public OperatorNode<QueryDerivedScan> {
 public:
  static Operator make(
      oid_t get_id, std::string alias,
      std::unordered_map<std::string,
                         std::shared_ptr<expression::AbstractExpression>>
          alias_to_expr_map);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  // identifier for all get operators
  oid_t get_id;
  std::string table_alias;
  std::unordered_map<std::string,
                     std::shared_ptr<expression::AbstractExpression>>
      alias_to_expr_map;
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
  static Operator make(
      int64_t offset, int64_t limit,
      std::vector<expression::AbstractExpression *> sort_columns,
      std::vector<bool> sort_ascending);
  int64_t offset;
  int64_t limit;
  // When we get a query like "SELECT * FROM tab ORDER BY a LIMIT 5"
  // We'll let the limit operator keep the order by clause's content as an
  // internal order, then the limit operator will generate sort plan with 
  // limit as a optimization.
  std::vector<expression::AbstractExpression *> sort_exprs;
  std::vector<bool> sort_acsending;
};

//===--------------------------------------------------------------------===//
// InnerNLJoin
//===--------------------------------------------------------------------===//
class PhysicalInnerNLJoin : public OperatorNode<PhysicalInnerNLJoin> {
 public:
  static Operator make(
      std::vector<AnnotatedExpression> conditions,
      std::vector<std::unique_ptr<expression::AbstractExpression>> &left_keys,
      std::vector<std::unique_ptr<expression::AbstractExpression>> &right_keys);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<std::unique_ptr<expression::AbstractExpression>> left_keys;
  std::vector<std::unique_ptr<expression::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
class PhysicalLeftNLJoin : public OperatorNode<PhysicalLeftNLJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(
      std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
class PhysicalRightNLJoin : public OperatorNode<PhysicalRightNLJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(
      std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
class PhysicalOuterNLJoin : public OperatorNode<PhysicalOuterNLJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(
      std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
class PhysicalInnerHashJoin : public OperatorNode<PhysicalInnerHashJoin> {
 public:
  static Operator make(
      std::vector<AnnotatedExpression> conditions,
      std::vector<std::unique_ptr<expression::AbstractExpression>> &left_keys,
      std::vector<std::unique_ptr<expression::AbstractExpression>> &right_keys);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  std::vector<std::unique_ptr<expression::AbstractExpression>> left_keys;
  std::vector<std::unique_ptr<expression::AbstractExpression>> right_keys;

  std::vector<AnnotatedExpression> join_predicates;
};

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
class PhysicalLeftHashJoin : public OperatorNode<PhysicalLeftHashJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(
      std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
class PhysicalRightHashJoin : public OperatorNode<PhysicalRightHashJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(
      std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
class PhysicalOuterHashJoin : public OperatorNode<PhysicalOuterHashJoin> {
 public:
  std::shared_ptr<expression::AbstractExpression> join_predicate;
  static Operator make(
      std::shared_ptr<expression::AbstractExpression> join_predicate);
};

//===--------------------------------------------------------------------===//
// PhysicalInsert
//===--------------------------------------------------------------------===//
class PhysicalInsert : public OperatorNode<PhysicalInsert> {
 public:
  static Operator make(
      std::shared_ptr<catalog::TableCatalogEntry> target_table,
      const std::vector<std::string> *columns,
      const std::vector<std::vector<
          std::unique_ptr<expression::AbstractExpression>>> *values);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  const std::vector<std::string> *columns;
  const std::vector<
      std::vector<std::unique_ptr<expression::AbstractExpression>>> *values;
};

class PhysicalInsertSelect : public OperatorNode<PhysicalInsertSelect> {
 public:
  static Operator make(
      std::shared_ptr<catalog::TableCatalogEntry> target_table);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
};

//===--------------------------------------------------------------------===//
// PhysicalDelete
//===--------------------------------------------------------------------===//
class PhysicalDelete : public OperatorNode<PhysicalDelete> {
 public:
  static Operator make(
      std::shared_ptr<catalog::TableCatalogEntry> target_table);
  std::shared_ptr<catalog::TableCatalogEntry> target_table;
};

//===--------------------------------------------------------------------===//
// PhysicalUpdate
//===--------------------------------------------------------------------===//
class PhysicalUpdate : public OperatorNode<PhysicalUpdate> {
 public:
  static Operator make(
      std::shared_ptr<catalog::TableCatalogEntry> target_table,
      const std::vector<std::unique_ptr<parser::UpdateClause>> *updates);

  std::shared_ptr<catalog::TableCatalogEntry> target_table;
  const std::vector<std::unique_ptr<parser::UpdateClause>> *updates;
};

//===--------------------------------------------------------------------===//
// Physical ExportExternalFile
//===--------------------------------------------------------------------===//
class PhysicalExportExternalFile
    : public OperatorNode<PhysicalExportExternalFile> {
 public:
  static Operator make(ExternalFileFormat format, std::string file_name,
                       char delimiter, char quote, char escape);

  bool operator==(const BaseOperatorNode &r) override;

  hash_t Hash() const override;

  ExternalFileFormat format;
  std::string file_name;
  char delimiter;
  char quote;
  char escape;
};

//===--------------------------------------------------------------------===//
// PhysicalHashGroupBy
//===--------------------------------------------------------------------===//
class PhysicalHashGroupBy : public OperatorNode<PhysicalHashGroupBy> {
 public:
  static Operator make(
      std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
      std::vector<AnnotatedExpression> having);

  bool operator==(const BaseOperatorNode &r) override;
  hash_t Hash() const override;

  std::vector<std::shared_ptr<expression::AbstractExpression>> columns;
  std::vector<AnnotatedExpression> having;
};

//===--------------------------------------------------------------------===//
// PhysicalSortGroupBy
//===--------------------------------------------------------------------===//
class PhysicalSortGroupBy : public OperatorNode<PhysicalSortGroupBy> {
 public:
  static Operator make(
      std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
      std::vector<AnnotatedExpression> having);

  bool operator==(const BaseOperatorNode &r) override;
  hash_t Hash() const override;
  // TODO(boweic): use raw ptr
  std::vector<std::shared_ptr<expression::AbstractExpression>> columns;
  std::vector<AnnotatedExpression> having;
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

}  // namespace optimizer
}  // namespace peloton
