//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operators.cpp
//
// Identification: src/optimizer/operators.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/operators.h"

#include "optimizer/operator_visitor.h"
#include "expression/expression_util.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Leaf
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LeafOperator::make(GroupID group) {
  LeafOperator *op = new LeafOperator;
  op->origin_group = group;
  return std::make_shared<Operator>(std::shared_ptr<LeafOperator>(op));
}

//===--------------------------------------------------------------------===//
// Get
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalGet::make(oid_t get_id,
                          std::vector<AnnotatedExpression> predicates,
                          std::shared_ptr<catalog::TableCatalogEntry> table,
                          std::string alias, bool update) {
  LogicalGet *get = new LogicalGet;
  get->table = table;
  get->table_alias = alias;
  get->predicates = std::move(predicates);
  get->is_for_update = update;
  get->get_id = get_id;
  util::to_lower_string(get->table_alias);
  return std::make_shared<Operator>(std::shared_ptr<LogicalGet>(get));
}

hash_t LogicalGet::Hash() const {
  hash_t hash = AbstractNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  for (auto &pred : predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalGet::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::Get) return false;
  const LogicalGet &node = *static_cast<const LogicalGet *>(&r);
  if (predicates.size() != node.predicates.size()) return false;
  for (size_t i = 0; i < predicates.size(); i++) {
    if (!predicates[i].expr->ExactlyEquals(*node.predicates[i].expr.get()))
      return false;
  }
  return get_id == node.get_id;
}

//===--------------------------------------------------------------------===//
// External file get
//===--------------------------------------------------------------------===//

std::shared_ptr<AbstractNode> LogicalExternalFileGet::make(
    oid_t get_id, ExternalFileFormat format, std::string file_name, char delimiter,
    char quote, char escape) {
  auto *get = new LogicalExternalFileGet();
  get->get_id = get_id;
  get->format = format;
  get->file_name = std::move(file_name);
  get->delimiter = delimiter;
  get->quote = quote;
  get->escape = escape;
  return std::make_shared<Operator>(std::shared_ptr<LogicalExternalFileGet>(get));
}

bool LogicalExternalFileGet::operator==(const AbstractNode &node) {
  if (node.GetOpType() != OpType::LogicalExternalFileGet) return false;
  const auto &get = *static_cast<const LogicalExternalFileGet *>(&node);
  return (get_id == get.get_id && format == get.format &&
          file_name == get.file_name && delimiter == get.delimiter &&
          quote == get.quote && escape == get.escape);
}

hash_t LogicalExternalFileGet::Hash() const {
  hash_t hash = AbstractNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&format));
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(file_name.data(), file_name.length()));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&delimiter, 1));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&quote, 1));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&escape, 1));
  return hash;
}

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalQueryDerivedGet::make(
    oid_t get_id, std::string &alias,
    std::unordered_map<std::string,
                       std::shared_ptr<expression::AbstractExpression>>
        alias_to_expr_map) {
  LogicalQueryDerivedGet *get = new LogicalQueryDerivedGet;
  get->table_alias = alias;
  get->alias_to_expr_map = alias_to_expr_map;
  get->get_id = get_id;

  return std::make_shared<Operator>(std::shared_ptr<LogicalQueryDerivedGet>(get));
}

bool LogicalQueryDerivedGet::operator==(const AbstractNode &node) {
  if (node.GetOpType() != OpType::LogicalQueryDerivedGet) return false;
  const LogicalQueryDerivedGet &r =
      *static_cast<const LogicalQueryDerivedGet *>(&node);
  return get_id == r.get_id;
}

hash_t LogicalQueryDerivedGet::Hash() const {
  hash_t hash = AbstractNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  return hash;
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalFilter::make(std::vector<AnnotatedExpression> &filter) {
  LogicalFilter *select = new LogicalFilter;
  select->predicates = std::move(filter);
  return std::make_shared<Operator>(std::shared_ptr<LogicalFilter>(select));
}

hash_t LogicalFilter::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &pred : predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalFilter::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::LogicalFilter) return false;
  const LogicalFilter &node = *static_cast<const LogicalFilter *>(&r);
  if (predicates.size() != node.predicates.size()) return false;
  for (size_t i = 0; i < predicates.size(); i++) {
    if (!predicates[i].expr->ExactlyEquals(*node.predicates[i].expr.get()))
      return false;
  }
  return true;
}
//===--------------------------------------------------------------------===//
// Project
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalProjection::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> &elements) {
  LogicalProjection *projection = new LogicalProjection;
  projection->expressions = std::move(elements);
  return std::make_shared<Operator>(std::shared_ptr<LogicalProjection>(projection));
}

//===--------------------------------------------------------------------===//
// DependentJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalDependentJoin::make() {
  LogicalDependentJoin *join = new LogicalDependentJoin;
  join->join_predicates = {};
  return std::make_shared<Operator>(std::shared_ptr<LogicalDependentJoin>(join));
}

std::shared_ptr<AbstractNode> LogicalDependentJoin::make(
    std::vector<AnnotatedExpression> &conditions) {
  LogicalDependentJoin *join = new LogicalDependentJoin;
  join->join_predicates = std::move(conditions);
  return std::make_shared<Operator>(std::shared_ptr<LogicalDependentJoin>(join));
}

hash_t LogicalDependentJoin::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalDependentJoin::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::LogicalDependentJoin) return false;
  const LogicalDependentJoin &node =
      *static_cast<const LogicalDependentJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size()) return false;
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(
            *node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// MarkJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalMarkJoin::make() {
  LogicalMarkJoin *join = new LogicalMarkJoin;
  join->join_predicates = {};
  return std::make_shared<Operator>(std::shared_ptr<LogicalMarkJoin>(join));
}

std::shared_ptr<AbstractNode> LogicalMarkJoin::make(std::vector<AnnotatedExpression> &conditions) {
  LogicalMarkJoin *join = new LogicalMarkJoin;
  join->join_predicates = std::move(conditions);
  return std::make_shared<Operator>(std::shared_ptr<LogicalMarkJoin>(join));
}

hash_t LogicalMarkJoin::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalMarkJoin::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::LogicalMarkJoin) return false;
  const LogicalMarkJoin &node = *static_cast<const LogicalMarkJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size()) return false;
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(
            *node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// SingleJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalSingleJoin::make() {
  LogicalMarkJoin *join = new LogicalMarkJoin;
  join->join_predicates = {};
  return std::make_shared<Operator>(std::shared_ptr<LogicalMarkJoin>(join));
}

std::shared_ptr<AbstractNode> LogicalSingleJoin::make(std::vector<AnnotatedExpression> &conditions) {
  LogicalSingleJoin *join = new LogicalSingleJoin;
  join->join_predicates = std::move(conditions);
  return std::make_shared<Operator>(std::shared_ptr<LogicalSingleJoin>(join));
}

hash_t LogicalSingleJoin::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalSingleJoin::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::LogicalSingleJoin) return false;
  const LogicalSingleJoin &node = *static_cast<const LogicalSingleJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size()) return false;
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(
            *node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalInnerJoin::make() {
  LogicalInnerJoin *join = new LogicalInnerJoin;
  join->join_predicates = {};
  return std::make_shared<Operator>(std::shared_ptr<LogicalInnerJoin>(join));
}

std::shared_ptr<AbstractNode> LogicalInnerJoin::make(std::vector<AnnotatedExpression> &conditions) {
  LogicalInnerJoin *join = new LogicalInnerJoin;
  join->join_predicates = std::move(conditions);
  return std::make_shared<Operator>(std::shared_ptr<LogicalInnerJoin>(join));
}

hash_t LogicalInnerJoin::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalInnerJoin::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::InnerJoin) return false;
  const LogicalInnerJoin &node = *static_cast<const LogicalInnerJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size()) return false;
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(
            *node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalLeftJoin::make(expression::AbstractExpression *condition) {
  LogicalLeftJoin *join = new LogicalLeftJoin;
  join->join_predicate =
      std::shared_ptr<expression::AbstractExpression>(condition);
  return std::make_shared<Operator>(std::shared_ptr<LogicalLeftJoin>(join));
}

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalRightJoin::make(expression::AbstractExpression *condition) {
  LogicalRightJoin *join = new LogicalRightJoin;
  join->join_predicate =
      std::shared_ptr<expression::AbstractExpression>(condition);
  return std::make_shared<Operator>(std::shared_ptr<LogicalRightJoin>(join));
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalOuterJoin::make(expression::AbstractExpression *condition) {
  LogicalOuterJoin *join = new LogicalOuterJoin;
  join->join_predicate =
      std::shared_ptr<expression::AbstractExpression>(condition);
  return std::make_shared<Operator>(std::shared_ptr<LogicalOuterJoin>(join));
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalSemiJoin::make(expression::AbstractExpression *condition) {
  LogicalSemiJoin *join = new LogicalSemiJoin;
  join->join_predicate =
      std::shared_ptr<expression::AbstractExpression>(condition);
  return std::make_shared<Operator>(std::shared_ptr<LogicalSemiJoin>(join));
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalAggregateAndGroupBy::make() {
  LogicalAggregateAndGroupBy *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns = {};
  return std::make_shared<Operator>(std::shared_ptr<LogicalAggregateAndGroupBy>(group_by));
}

std::shared_ptr<AbstractNode> LogicalAggregateAndGroupBy::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> &columns) {
  LogicalAggregateAndGroupBy *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns = move(columns);
  return std::make_shared<Operator>(std::shared_ptr<LogicalAggregateAndGroupBy>(group_by));
}

std::shared_ptr<AbstractNode> LogicalAggregateAndGroupBy::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> &columns,
    std::vector<AnnotatedExpression> &having) {
  LogicalAggregateAndGroupBy *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns = move(columns);
  group_by->having = move(having);
  return std::make_shared<Operator>(std::shared_ptr<LogicalAggregateAndGroupBy>(group_by));
}

bool LogicalAggregateAndGroupBy::operator==(const AbstractNode &node) {
  if (node.GetOpType() != OpType::LogicalAggregateAndGroupBy) return false;
  const LogicalAggregateAndGroupBy &r =
      *static_cast<const LogicalAggregateAndGroupBy *>(&node);
  if (having.size() != r.having.size() || columns.size() != r.columns.size())
    return false;
  for (size_t i = 0; i < having.size(); i++) {
    if (!having[i].expr->ExactlyEquals(*r.having[i].expr.get())) return false;
  }
  return expression::ExpressionUtil::EqualExpressions(columns, r.columns);
}

hash_t LogicalAggregateAndGroupBy::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &pred : having) hash = HashUtil::SumHashes(hash, pred.expr->Hash());
  for (auto expr : columns) hash = HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalInsert::make(
    std::shared_ptr<catalog::TableCatalogEntry> target_table,
    const std::vector<std::string> *columns,
    const std::vector<std::vector<
        std::unique_ptr<peloton::expression::AbstractExpression>>> *values) {
  LogicalInsert *insert_op = new LogicalInsert;
  insert_op->target_table = target_table;
  insert_op->columns = columns;
  insert_op->values = values;
  return std::make_shared<Operator>(std::shared_ptr<LogicalInsert>(insert_op));
}

std::shared_ptr<AbstractNode> LogicalInsertSelect::make(
    std::shared_ptr<catalog::TableCatalogEntry> target_table) {
  LogicalInsertSelect *insert_op = new LogicalInsertSelect;
  insert_op->target_table = target_table;
  return std::make_shared<Operator>(std::shared_ptr<LogicalInsertSelect>(insert_op));
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalDelete::make(
    std::shared_ptr<catalog::TableCatalogEntry> target_table) {
  LogicalDelete *delete_op = new LogicalDelete;
  delete_op->target_table = target_table;
  return std::make_shared<Operator>(std::shared_ptr<LogicalDelete>(delete_op));
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalUpdate::make(
    std::shared_ptr<catalog::TableCatalogEntry> target_table,
    const std::vector<std::unique_ptr<peloton::parser::UpdateClause>> *
        updates) {
  LogicalUpdate *update_op = new LogicalUpdate;
  update_op->target_table = target_table;
  update_op->updates = updates;
  return std::make_shared<Operator>(std::shared_ptr<LogicalUpdate>(update_op));
}

//===--------------------------------------------------------------------===//
// Distinct
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalDistinct::make() {
  return std::make_shared<LogicalDistinct>();
}

//===--------------------------------------------------------------------===//
// Limit
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalLimit::make(
    int64_t offset, int64_t limit,
    std::vector<expression::AbstractExpression *> &&sort_exprs,
    std::vector<bool> &&sort_ascending) {
  LogicalLimit *limit_op = new LogicalLimit;
  limit_op->offset = offset;
  limit_op->limit = limit;
  limit_op->sort_exprs = std::move(sort_exprs);
  limit_op->sort_ascending = std::move(sort_ascending);
  return std::make_shared<Operator>(std::shared_ptr<LogicalLimit>(limit_op));
}

//===--------------------------------------------------------------------===//
// External file output
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> LogicalExportExternalFile::make(ExternalFileFormat format,
                                         std::string file_name, char delimiter,
                                         char quote, char escape) {
  auto *export_op = new LogicalExportExternalFile();
  export_op->format = format;
  export_op->file_name = std::move(file_name);
  export_op->delimiter = delimiter;
  export_op->quote = quote;
  export_op->escape = escape;
  return std::make_shared<Operator>(std::shared_ptr<LogicalExportExternalFile>(export_op));
}

bool LogicalExportExternalFile::operator==(const AbstractNode &node) {
  if (node.GetOpType() != OpType::LogicalExportExternalFile) return false;
  const auto &export_op =
      *static_cast<const LogicalExportExternalFile *>(&node);
  return (format == export_op.format && file_name == export_op.file_name &&
          delimiter == export_op.delimiter && quote == export_op.quote &&
          escape == export_op.escape);
}

hash_t LogicalExportExternalFile::Hash() const {
  hash_t hash = AbstractNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&format));
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(file_name.data(), file_name.length()));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&delimiter, 1));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&quote, 1));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&escape, 1));
  return hash;
}

//===--------------------------------------------------------------------===//
// DummyScan
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> DummyScan::make() {
  return std::make_shared<DummyScan>();
}

//===--------------------------------------------------------------------===//
// SeqScan
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalSeqScan::make(
    oid_t get_id, std::shared_ptr<catalog::TableCatalogEntry> table,
    std::string alias, std::vector<AnnotatedExpression> predicates,
    bool update) {
  PELOTON_ASSERT(table != nullptr);
  PhysicalSeqScan *scan = new PhysicalSeqScan;
  scan->table_ = table;
  scan->table_alias = alias;
  scan->predicates = std::move(predicates);
  scan->is_for_update = update;
  scan->get_id = get_id;

  return std::make_shared<Operator>(std::shared_ptr<PhysicalSeqScan>(scan));
}

bool PhysicalSeqScan::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::SeqScan) return false;
  const PhysicalSeqScan &node = *static_cast<const PhysicalSeqScan *>(&r);
  if (predicates.size() != node.predicates.size()) return false;
  for (size_t i = 0; i < predicates.size(); i++) {
    if (!predicates[i].expr->ExactlyEquals(*node.predicates[i].expr.get()))
      return false;
  }
  return get_id == node.get_id;
}

hash_t PhysicalSeqScan::Hash() const {
  hash_t hash = AbstractNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  for (auto &pred : predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalIndexScan::make(
    oid_t get_id, std::shared_ptr<catalog::TableCatalogEntry> table,
    std::string alias, std::vector<AnnotatedExpression> predicates, bool update,
    oid_t index_id, std::vector<oid_t> key_column_id_list,
    std::vector<ExpressionType> expr_type_list,
    std::vector<type::Value> value_list) {
  PELOTON_ASSERT(table != nullptr);
  PhysicalIndexScan *scan = new PhysicalIndexScan;
  scan->table_ = table;
  scan->is_for_update = update;
  scan->predicates = std::move(predicates);
  scan->table_alias = std::move(alias);
  scan->get_id = get_id;
  scan->index_id = index_id;
  scan->key_column_id_list = std::move(key_column_id_list);
  scan->expr_type_list = std::move(expr_type_list);
  scan->value_list = std::move(value_list);

  return std::make_shared<Operator>(std::shared_ptr<PhysicalIndexScan>(scan));
}

bool PhysicalIndexScan::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::IndexScan) return false;
  const PhysicalIndexScan &node = *static_cast<const PhysicalIndexScan *>(&r);
  // TODO: Should also check value list
  if (index_id != node.index_id ||
      key_column_id_list != node.key_column_id_list ||
      expr_type_list != node.expr_type_list ||
      predicates.size() != node.predicates.size())
    return false;

  for (size_t i = 0; i < predicates.size(); i++) {
    if (!predicates[i].expr->ExactlyEquals(*node.predicates[i].expr.get()))
      return false;
  }
  return get_id == node.get_id;
}

hash_t PhysicalIndexScan::Hash() const {
  hash_t hash = AbstractNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&index_id));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  for (auto &pred : predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// Physical external file scan
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> ExternalFileScan::make(oid_t get_id, ExternalFileFormat format,
                                std::string file_name, char delimiter,
                                char quote, char escape) {
  auto *get = new ExternalFileScan();
  get->get_id = get_id;
  get->format = format;
  get->file_name = file_name;
  get->delimiter = delimiter;
  get->quote = quote;
  get->escape = escape;
  return std::make_shared<Operator>(std::shared_ptr<ExternalFileScan>(get));
}

bool ExternalFileScan::operator==(const AbstractNode &node) {
  if (node.GetOpType() != OpType::QueryDerivedScan) return false;
  const auto &get = *static_cast<const ExternalFileScan *>(&node);
  return (get_id == get.get_id && format == get.format &&
          file_name == get.file_name && delimiter == get.delimiter &&
          quote == get.quote && escape == get.escape);
}

hash_t ExternalFileScan::Hash() const {
  hash_t hash = AbstractNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&format));
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(file_name.data(), file_name.length()));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&delimiter, 1));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&quote, 1));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&escape, 1));
  return hash;
}

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> QueryDerivedScan::make(
    oid_t get_id, std::string alias,
    std::unordered_map<std::string,
                       std::shared_ptr<expression::AbstractExpression>>
        alias_to_expr_map) {
  QueryDerivedScan *get = new QueryDerivedScan;
  get->table_alias = alias;
  get->alias_to_expr_map = alias_to_expr_map;
  get->get_id = get_id;

  return std::make_shared<Operator>(std::shared_ptr<QueryDerivedScan>(get));
}

bool QueryDerivedScan::operator==(const AbstractNode &node) {
  if (node.GetOpType() != OpType::QueryDerivedScan) return false;
  const QueryDerivedScan &r = *static_cast<const QueryDerivedScan *>(&node);
  return get_id == r.get_id;
}

hash_t QueryDerivedScan::Hash() const {
  hash_t hash = AbstractNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  return hash;
}

//===--------------------------------------------------------------------===//
// OrderBy
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalOrderBy::make() {
  return std::make_shared<PhysicalOrderBy>();
}

//===--------------------------------------------------------------------===//
// PhysicalLimit
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalLimit::make(
    int64_t offset, int64_t limit,
    std::vector<expression::AbstractExpression *> sort_exprs,
    std::vector<bool> sort_ascending) {
  PhysicalLimit *limit_op = new PhysicalLimit;
  limit_op->offset = offset;
  limit_op->limit = limit;
  limit_op->sort_exprs = sort_exprs;
  limit_op->sort_acsending = sort_ascending;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalLimit>(limit_op));
}

//===--------------------------------------------------------------------===//
// InnerNLJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalInnerNLJoin::make(
    std::vector<AnnotatedExpression> conditions,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &left_keys,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &right_keys) {
  PhysicalInnerNLJoin *join = new PhysicalInnerNLJoin();
  join->join_predicates = std::move(conditions);
  join->left_keys = std::move(left_keys);
  join->right_keys = std::move(right_keys);

  return std::make_shared<Operator>(std::shared_ptr<PhysicalInnerNLJoin>(join));
}

hash_t PhysicalInnerNLJoin::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &expr : left_keys)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool PhysicalInnerNLJoin::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::InnerNLJoin) return false;
  const PhysicalInnerNLJoin &node =
      *static_cast<const PhysicalInnerNLJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size() ||
      left_keys.size() != node.left_keys.size() ||
      right_keys.size() != node.right_keys.size())
    return false;
  for (size_t i = 0; i < left_keys.size(); i++) {
    if (!left_keys[i]->ExactlyEquals(*node.left_keys[i].get())) return false;
  }
  for (size_t i = 0; i < right_keys.size(); i++) {
    if (!right_keys[i]->ExactlyEquals(*node.right_keys[i].get())) return false;
  }
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(
            *node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalLeftNLJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalLeftNLJoin *join = new PhysicalLeftNLJoin();
  join->join_predicate = join_predicate;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalLeftNLJoin>(join));
}

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalRightNLJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalRightNLJoin *join = new PhysicalRightNLJoin();
  join->join_predicate = join_predicate;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalRightNLJoin>(join));
}

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalOuterNLJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalOuterNLJoin *join = new PhysicalOuterNLJoin();
  join->join_predicate = join_predicate;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalOuterNLJoin>(join));
}

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalInnerHashJoin::make(
    std::vector<AnnotatedExpression> conditions,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &left_keys,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &right_keys) {
  PhysicalInnerHashJoin *join = new PhysicalInnerHashJoin();
  join->join_predicates = std::move(conditions);
  join->left_keys = std::move(left_keys);
  join->right_keys = std::move(right_keys);
  return std::make_shared<Operator>(std::shared_ptr<PhysicalInnerHashJoin>(join));
}

hash_t PhysicalInnerHashJoin::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &expr : left_keys)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool PhysicalInnerHashJoin::operator==(const AbstractNode &r) {
  if (r.GetOpType() != OpType::InnerHashJoin) return false;
  const PhysicalInnerHashJoin &node =
      *static_cast<const PhysicalInnerHashJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size() ||
      left_keys.size() != node.left_keys.size() ||
      right_keys.size() != node.right_keys.size())
    return false;
  for (size_t i = 0; i < left_keys.size(); i++) {
    if (!left_keys[i]->ExactlyEquals(*node.left_keys[i].get())) return false;
  }
  for (size_t i = 0; i < right_keys.size(); i++) {
    if (!right_keys[i]->ExactlyEquals(*node.right_keys[i].get())) return false;
  }
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(
            *node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalLeftHashJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalLeftHashJoin *join = new PhysicalLeftHashJoin();
  join->join_predicate = join_predicate;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalLeftHashJoin>(join));
}

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalRightHashJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalRightHashJoin *join = new PhysicalRightHashJoin();
  join->join_predicate = join_predicate;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalRightHashJoin>(join));
}

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalOuterHashJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalOuterHashJoin *join = new PhysicalOuterHashJoin();
  join->join_predicate = join_predicate;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalOuterHashJoin>(join));
}

//===--------------------------------------------------------------------===//
// PhysicalInsert
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalInsert::make(
    std::shared_ptr<catalog::TableCatalogEntry> target_table,
    const std::vector<std::string> *columns,
    const std::vector<std::vector<
        std::unique_ptr<peloton::expression::AbstractExpression>>> *values) {
  PhysicalInsert *insert_op = new PhysicalInsert;
  insert_op->target_table = target_table;
  insert_op->columns = columns;
  insert_op->values = values;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalInsert>(insert_op));
}

//===--------------------------------------------------------------------===//
// PhysicalInsertSelect
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalInsertSelect::make(
    std::shared_ptr<catalog::TableCatalogEntry> target_table) {
  PhysicalInsertSelect *insert_op = new PhysicalInsertSelect;
  insert_op->target_table = target_table;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalInsertSelect>(insert_op));
}

//===--------------------------------------------------------------------===//
// PhysicalDelete
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalDelete::make(
    std::shared_ptr<catalog::TableCatalogEntry> target_table) {
  PhysicalDelete *delete_op = new PhysicalDelete;
  delete_op->target_table = target_table;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalDelete>(delete_op));
}

//===--------------------------------------------------------------------===//
// PhysicalUpdate
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalUpdate::make(
    std::shared_ptr<catalog::TableCatalogEntry> target_table,
    const std::vector<std::unique_ptr<peloton::parser::UpdateClause>> *
        updates) {
  PhysicalUpdate *update = new PhysicalUpdate;
  update->target_table = target_table;
  update->updates = updates;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalUpdate>(update));
}

//===--------------------------------------------------------------------===//
// PhysicalExportExternalFile
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalExportExternalFile::make(ExternalFileFormat format,
                                          std::string file_name, char delimiter,
                                          char quote, char escape) {
  auto *export_op = new PhysicalExportExternalFile();
  export_op->format = format;
  export_op->file_name = file_name;
  export_op->delimiter = delimiter;
  export_op->quote = quote;
  export_op->escape = escape;
  return std::make_shared<Operator>(std::shared_ptr<PhysicalExportExternalFile>(export_op));
}

bool PhysicalExportExternalFile::operator==(const AbstractNode &node) {
  if (node.GetOpType() != OpType::ExportExternalFile) return false;
  const auto &export_op =
      *static_cast<const PhysicalExportExternalFile *>(&node);
  return (format == export_op.format && file_name == export_op.file_name &&
          delimiter == export_op.delimiter && quote == export_op.quote &&
          escape == export_op.escape);
}

hash_t PhysicalExportExternalFile::Hash() const {
  hash_t hash = AbstractNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&format));
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(file_name.data(), file_name.length()));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&delimiter, 1));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&quote, 1));
  hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(&escape, 1));
  return hash;
}

//===--------------------------------------------------------------------===//
// PhysicalHashGroupBy
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalHashGroupBy::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
    std::vector<AnnotatedExpression> having) {
  PhysicalHashGroupBy *agg = new PhysicalHashGroupBy;
  agg->columns = columns;
  agg->having = move(having);
  return std::make_shared<Operator>(std::shared_ptr<PhysicalHashGroupBy>(agg));
}

bool PhysicalHashGroupBy::operator==(const AbstractNode &node) {
  if (node.GetOpType() != OpType::HashGroupBy) return false;
  const PhysicalHashGroupBy &r =
      *static_cast<const PhysicalHashGroupBy *>(&node);
  if (having.size() != r.having.size() || columns.size() != r.columns.size())
    return false;
  for (size_t i = 0; i < having.size(); i++) {
    if (!having[i].expr->ExactlyEquals(*r.having[i].expr.get())) return false;
  }
  return expression::ExpressionUtil::EqualExpressions(columns, r.columns);
}

hash_t PhysicalHashGroupBy::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &pred : having) hash = HashUtil::SumHashes(hash, pred.expr->Hash());
  for (auto expr : columns) hash = HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// PhysicalSortGroupBy
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalSortGroupBy::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
    std::vector<AnnotatedExpression> having) {
  PhysicalSortGroupBy *agg = new PhysicalSortGroupBy;
  agg->columns = std::move(columns);
  agg->having = move(having);
  return std::make_shared<Operator>(std::shared_ptr<PhysicalSortGroupBy>(agg));
}

bool PhysicalSortGroupBy::operator==(const AbstractNode &node) {
  if (node.GetOpType() != OpType::SortGroupBy) return false;
  const PhysicalSortGroupBy &r =
      *static_cast<const PhysicalSortGroupBy *>(&node);
  if (having.size() != r.having.size() || columns.size() != r.columns.size())
    return false;
  for (size_t i = 0; i < having.size(); i++) {
    if (!having[i].expr->ExactlyEquals(*r.having[i].expr.get())) return false;
  }
  return expression::ExpressionUtil::EqualExpressions(columns, r.columns);
}

hash_t PhysicalSortGroupBy::Hash() const {
  hash_t hash = AbstractNode::Hash();
  for (auto &pred : having) hash = HashUtil::SumHashes(hash, pred.expr->Hash());
  for (auto expr : columns) hash = HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// PhysicalAggregate
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalAggregate::make() {
  return std::make_shared<PhysicalAggregate>();
}

//===--------------------------------------------------------------------===//
// Physical Hash
//===--------------------------------------------------------------------===//
std::shared_ptr<AbstractNode> PhysicalDistinct::make() {
  return std::make_shared<PhysicalDistinct>();
}

//===--------------------------------------------------------------------===//
template <typename T>
void OperatorNode<T>::Accept(OperatorVisitor *v) const {
  v->Visit((const T *)this);
}

//===--------------------------------------------------------------------===//
template <>
std::string OperatorNode<LeafOperator>::name_ = "LeafOperator";
template <>
std::string OperatorNode<LogicalGet>::name_ = "LogicalGet";
template <>
std::string OperatorNode<LogicalExternalFileGet>::name_ =
    "LogicalExternalFileGet";
template <>
std::string OperatorNode<LogicalQueryDerivedGet>::name_ =
    "LogicalQueryDerivedGet";
template <>
std::string OperatorNode<LogicalFilter>::name_ = "LogicalFilter";
template <>
std::string OperatorNode<LogicalProjection>::name_ = "LogicalProjection";
template <>
std::string OperatorNode<LogicalMarkJoin>::name_ = "LogicalMarkJoin";
template <>
std::string OperatorNode<LogicalSingleJoin>::name_ = "LogicalSingleJoin";
template <>
std::string OperatorNode<LogicalDependentJoin>::name_ = "LogicalDependentJoin";
template <>
std::string OperatorNode<LogicalInnerJoin>::name_ = "LogicalInnerJoin";
template <>
std::string OperatorNode<LogicalLeftJoin>::name_ = "LogicalLeftJoin";
template <>
std::string OperatorNode<LogicalRightJoin>::name_ = "LogicalRightJoin";
template <>
std::string OperatorNode<LogicalOuterJoin>::name_ = "LogicalOuterJoin";
template <>
std::string OperatorNode<LogicalSemiJoin>::name_ = "LogicalSemiJoin";
template <>
std::string OperatorNode<LogicalAggregateAndGroupBy>::name_ =
    "LogicalAggregateAndGroupBy";
template <>
std::string OperatorNode<LogicalInsert>::name_ = "LogicalInsert";
template <>
std::string OperatorNode<LogicalInsertSelect>::name_ = "LogicalInsertSelect";
template <>
std::string OperatorNode<LogicalUpdate>::name_ = "LogicalUpdate";
template <>
std::string OperatorNode<LogicalDelete>::name_ = "LogicalDelete";
template <>
std::string OperatorNode<LogicalLimit>::name_ = "LogicalLimit";
template <>
std::string OperatorNode<LogicalDistinct>::name_ = "LogicalDistinct";
template <>
std::string OperatorNode<LogicalExportExternalFile>::name_ =
    "LogicalExportExternalFile";
template <>
std::string OperatorNode<DummyScan>::name_ = "DummyScan";
template <>
std::string OperatorNode<PhysicalSeqScan>::name_ = "PhysicalSeqScan";
template <>
std::string OperatorNode<PhysicalIndexScan>::name_ = "PhysicalIndexScan";
template <>
std::string OperatorNode<ExternalFileScan>::name_ = "ExternalFileScan";
template <>
std::string OperatorNode<QueryDerivedScan>::name_ = "QueryDerivedScan";
template <>
std::string OperatorNode<PhysicalOrderBy>::name_ = "PhysicalOrderBy";
template <>
std::string OperatorNode<PhysicalLimit>::name_ = "PhysicalLimit";
template <>
std::string OperatorNode<PhysicalInnerNLJoin>::name_ = "PhysicalInnerNLJoin";
template <>
std::string OperatorNode<PhysicalLeftNLJoin>::name_ = "PhysicalLeftNLJoin";
template <>
std::string OperatorNode<PhysicalRightNLJoin>::name_ = "PhysicalRightNLJoin";
template <>
std::string OperatorNode<PhysicalOuterNLJoin>::name_ = "PhysicalOuterNLJoin";
template <>
std::string OperatorNode<PhysicalInnerHashJoin>::name_ =
    "PhysicalInnerHashJoin";
template <>
std::string OperatorNode<PhysicalLeftHashJoin>::name_ = "PhysicalLeftHashJoin";
template <>
std::string OperatorNode<PhysicalRightHashJoin>::name_ =
    "PhysicalRightHashJoin";
template <>
std::string OperatorNode<PhysicalOuterHashJoin>::name_ =
    "PhysicalOuterHashJoin";
template <>
std::string OperatorNode<PhysicalInsert>::name_ = "PhysicalInsert";
template <>
std::string OperatorNode<PhysicalInsertSelect>::name_ = "PhysicalInsertSelect";
template <>
std::string OperatorNode<PhysicalDelete>::name_ = "PhysicalDelete";
template <>
std::string OperatorNode<PhysicalUpdate>::name_ = "PhysicalUpdate";
template <>
std::string OperatorNode<PhysicalHashGroupBy>::name_ = "PhysicalHashGroupBy";
template <>
std::string OperatorNode<PhysicalSortGroupBy>::name_ = "PhysicalSortGroupBy";
template <>
std::string OperatorNode<PhysicalDistinct>::name_ = "PhysicalDistinct";
template <>
std::string OperatorNode<PhysicalAggregate>::name_ = "PhysicalAggregate";
template <>
std::string OperatorNode<PhysicalExportExternalFile>::name_ =
    "PhysicalExportExternalFile";

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNode<LeafOperator>::op_type_ = OpType::Leaf;
template <>
OpType OperatorNode<LogicalGet>::op_type_ = OpType::Get;
template <>
OpType OperatorNode<LogicalExternalFileGet>::op_type_ =
    OpType::LogicalExternalFileGet;
template <>
OpType OperatorNode<LogicalQueryDerivedGet>::op_type_ =
    OpType::LogicalQueryDerivedGet;
template <>
OpType OperatorNode<LogicalFilter>::op_type_ = OpType::LogicalFilter;
template <>
OpType OperatorNode<LogicalProjection>::op_type_ = OpType::LogicalProjection;
template <>
OpType OperatorNode<LogicalMarkJoin>::op_type_ = OpType::LogicalMarkJoin;
template <>
OpType OperatorNode<LogicalSingleJoin>::op_type_ = OpType::LogicalSingleJoin;
template <>
OpType OperatorNode<LogicalDependentJoin>::op_type_ = OpType::LogicalDependentJoin;
template <>
OpType OperatorNode<LogicalInnerJoin>::op_type_ = OpType::InnerJoin;
template <>
OpType OperatorNode<LogicalLeftJoin>::op_type_ = OpType::LeftJoin;
template <>
OpType OperatorNode<LogicalRightJoin>::op_type_ = OpType::RightJoin;
template <>
OpType OperatorNode<LogicalOuterJoin>::op_type_ = OpType::OuterJoin;
template <>
OpType OperatorNode<LogicalSemiJoin>::op_type_ = OpType::SemiJoin;
template <>
OpType OperatorNode<LogicalAggregateAndGroupBy>::op_type_ =
    OpType::LogicalAggregateAndGroupBy;
template <>
OpType OperatorNode<LogicalInsert>::op_type_ = OpType::LogicalInsert;
template <>
OpType OperatorNode<LogicalInsertSelect>::op_type_ = OpType::LogicalInsertSelect;
template <>
OpType OperatorNode<LogicalUpdate>::op_type_ = OpType::LogicalUpdate;
template <>
OpType OperatorNode<LogicalDelete>::op_type_ = OpType::LogicalDelete;
template <>
OpType OperatorNode<LogicalDistinct>::op_type_ = OpType::LogicalDistinct;
template <>
OpType OperatorNode<LogicalLimit>::op_type_ = OpType::LogicalLimit;
template <>
OpType OperatorNode<LogicalExportExternalFile>::op_type_ =
    OpType::LogicalExportExternalFile;

template <>
OpType OperatorNode<DummyScan>::op_type_ = OpType::DummyScan;
template <>
OpType OperatorNode<PhysicalSeqScan>::op_type_ = OpType::SeqScan;
template <>
OpType OperatorNode<PhysicalIndexScan>::op_type_ = OpType::IndexScan;
template <>
OpType OperatorNode<ExternalFileScan>::op_type_ = OpType::ExternalFileScan;
template <>
OpType OperatorNode<QueryDerivedScan>::op_type_ = OpType::QueryDerivedScan;
template <>
OpType OperatorNode<PhysicalOrderBy>::op_type_ = OpType::OrderBy;
template <>
OpType OperatorNode<PhysicalDistinct>::op_type_ = OpType::Distinct;
template <>
OpType OperatorNode<PhysicalLimit>::op_type_ = OpType::PhysicalLimit;
template <>
OpType OperatorNode<PhysicalInnerNLJoin>::op_type_ = OpType::InnerNLJoin;
template <>
OpType OperatorNode<PhysicalLeftNLJoin>::op_type_ = OpType::LeftNLJoin;
template <>
OpType OperatorNode<PhysicalRightNLJoin>::op_type_ = OpType::RightNLJoin;
template <>
OpType OperatorNode<PhysicalOuterNLJoin>::op_type_ = OpType::OuterNLJoin;
template <>
OpType OperatorNode<PhysicalInnerHashJoin>::op_type_ = OpType::InnerHashJoin;
template <>
OpType OperatorNode<PhysicalLeftHashJoin>::op_type_ = OpType::LeftHashJoin;
template <>
OpType OperatorNode<PhysicalRightHashJoin>::op_type_ = OpType::RightHashJoin;
template <>
OpType OperatorNode<PhysicalOuterHashJoin>::op_type_ = OpType::OuterHashJoin;
template <>
OpType OperatorNode<PhysicalInsert>::op_type_ = OpType::Insert;
template <>
OpType OperatorNode<PhysicalInsertSelect>::op_type_ = OpType::InsertSelect;
template <>
OpType OperatorNode<PhysicalDelete>::op_type_ = OpType::Delete;
template <>
OpType OperatorNode<PhysicalUpdate>::op_type_ = OpType::Update;
template <>
OpType OperatorNode<PhysicalHashGroupBy>::op_type_ = OpType::HashGroupBy;
template <>
OpType OperatorNode<PhysicalSortGroupBy>::op_type_ = OpType::SortGroupBy;
template <>
OpType OperatorNode<PhysicalAggregate>::op_type_ = OpType::Aggregate;
template <>
OpType OperatorNode<PhysicalExportExternalFile>::op_type_ =
    OpType::ExportExternalFile;

//===--------------------------------------------------------------------===//
template <typename T>
ExpressionType OperatorNode<T>::exp_type_ = ExpressionType::INVALID;

//===--------------------------------------------------------------------===//

template <typename T>
bool OperatorNode<T>::IsLogical() const {
  return op_type_ < OpType::LogicalPhysicalDelimiter;
}

template <typename T>
bool OperatorNode<T>::IsPhysical() const {
  return op_type_ > OpType::LogicalPhysicalDelimiter;
}

template <>
bool OperatorNode<LeafOperator>::IsLogical() const {
  return false;
}

template <>
bool OperatorNode<LeafOperator>::IsPhysical() const {
  return false;
}

}  // namespace optimizer
}  // namespace peloton
