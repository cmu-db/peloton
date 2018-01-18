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
Operator LeafOperator::make(GroupID group) {
  LeafOperator *op = new LeafOperator;
  op->origin_group = group;
  return Operator(op);
}

//===--------------------------------------------------------------------===//
// Get
//===--------------------------------------------------------------------===//
Operator LogicalGet::make(oid_t get_id,
                          std::vector<AnnotatedExpression> predicates,
                          std::shared_ptr<catalog::TableCatalogObject> table, std::string alias,
                          bool update) {
  LogicalGet *get = new LogicalGet;
  get->table = table;
  get->table_alias = alias;
  get->predicates = std::move(predicates);
  get->is_for_update = update;
  get->get_id = get_id;
  util::to_lower_string(get->table_alias);
  return Operator(get);
}

hash_t LogicalGet::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  for (auto &pred : predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalGet::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::Get) return false;
  const LogicalGet &node = *static_cast<const LogicalGet *>(&r);
  if (predicates.size() != node.predicates.size()) return false;
  for (size_t i = 0; i < predicates.size(); i++) {
    if (!predicates[i].expr->ExactlyEquals(*node.predicates[i].expr.get()))
      return false;
  }
  return get_id == node.get_id;
}

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
Operator LogicalQueryDerivedGet::make(
    oid_t get_id, std::string &alias,
    std::unordered_map<std::string,
                       std::shared_ptr<expression::AbstractExpression>>
        alias_to_expr_map) {
  LogicalQueryDerivedGet *get = new LogicalQueryDerivedGet;
  get->table_alias = alias;
  get->alias_to_expr_map = alias_to_expr_map;
  get->get_id = get_id;

  return Operator(get);
}

bool LogicalQueryDerivedGet::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::LogicalQueryDerivedGet) return false;
  const LogicalQueryDerivedGet &r =
      *static_cast<const LogicalQueryDerivedGet *>(&node);
  return get_id == r.get_id;
}

hash_t LogicalQueryDerivedGet::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  return hash;
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
Operator LogicalFilter::make(std::vector<AnnotatedExpression> &filter) {
  LogicalFilter *select = new LogicalFilter;
  select->predicates = std::move(filter);
  return Operator(select);
}

hash_t LogicalFilter::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalFilter::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::LogicalFilter) return false;
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
Operator LogicalProjection::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> &elements) {
  LogicalProjection *projection = new LogicalProjection;
  projection->expressions = std::move(elements);
  return Operator(projection);
}

//===--------------------------------------------------------------------===//
// DependentJoin
//===--------------------------------------------------------------------===//
Operator LogicalDependentJoin::make() {
  LogicalDependentJoin *join = new LogicalDependentJoin;
  join->join_predicates = {};
  return Operator(join);
}

Operator LogicalDependentJoin::make(
    std::vector<AnnotatedExpression> &conditions) {
  LogicalDependentJoin *join = new LogicalDependentJoin;
  join->join_predicates = std::move(conditions);
  return Operator(join);
}

hash_t LogicalDependentJoin::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalDependentJoin::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::LogicalDependentJoin) return false;
  const LogicalDependentJoin &node =
      *static_cast<const LogicalDependentJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size()) return false;
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(*node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// MarkJoin
//===--------------------------------------------------------------------===//
Operator LogicalMarkJoin::make() {
  LogicalMarkJoin *join = new LogicalMarkJoin;
  join->join_predicates = {};
  return Operator(join);
}

Operator LogicalMarkJoin::make(std::vector<AnnotatedExpression> &conditions) {
  LogicalMarkJoin *join = new LogicalMarkJoin;
  join->join_predicates = std::move(conditions);
  return Operator(join);
}

hash_t LogicalMarkJoin::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalMarkJoin::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::LogicalMarkJoin) return false;
  const LogicalMarkJoin &node = *static_cast<const LogicalMarkJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size()) return false;
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(*node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}


//===--------------------------------------------------------------------===//
// SingleJoin
//===--------------------------------------------------------------------===//
Operator LogicalSingleJoin::make() {
  LogicalMarkJoin *join = new LogicalMarkJoin;
  join->join_predicates = {};
  return Operator(join);
}

Operator LogicalSingleJoin::make(std::vector<AnnotatedExpression> &conditions) {
  LogicalSingleJoin *join = new LogicalSingleJoin;
  join->join_predicates = std::move(conditions);
  return Operator(join);
}

hash_t LogicalSingleJoin::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalSingleJoin::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::LogicalSingleJoin) return false;
  const LogicalSingleJoin &node = *static_cast<const LogicalSingleJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size()) return false;
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(*node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
Operator LogicalInnerJoin::make() {
  LogicalInnerJoin *join = new LogicalInnerJoin;
  join->join_predicates = {};
  return Operator(join);
}

Operator LogicalInnerJoin::make(std::vector<AnnotatedExpression> &conditions) {
  LogicalInnerJoin *join = new LogicalInnerJoin;
  join->join_predicates = std::move(conditions);
  return Operator(join);
}

hash_t LogicalInnerJoin::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool LogicalInnerJoin::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::InnerJoin) return false;
  const LogicalInnerJoin &node = *static_cast<const LogicalInnerJoin *>(&r);
  if (join_predicates.size() != node.join_predicates.size()) return false;
  for (size_t i = 0; i < join_predicates.size(); i++) {
    if (!join_predicates[i].expr->ExactlyEquals(*node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
Operator LogicalLeftJoin::make(expression::AbstractExpression *condition) {
  LogicalLeftJoin *join = new LogicalLeftJoin;
  join->join_predicate =
      std::shared_ptr<expression::AbstractExpression>(condition);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
Operator LogicalRightJoin::make(expression::AbstractExpression *condition) {
  LogicalRightJoin *join = new LogicalRightJoin;
  join->join_predicate =
      std::shared_ptr<expression::AbstractExpression>(condition);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
Operator LogicalOuterJoin::make(expression::AbstractExpression *condition) {
  LogicalOuterJoin *join = new LogicalOuterJoin;
  join->join_predicate =
      std::shared_ptr<expression::AbstractExpression>(condition);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
Operator LogicalSemiJoin::make(expression::AbstractExpression *condition) {
  LogicalSemiJoin *join = new LogicalSemiJoin;
  join->join_predicate =
      std::shared_ptr<expression::AbstractExpression>(condition);
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
Operator LogicalAggregateAndGroupBy::make() {
  LogicalAggregateAndGroupBy *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns = {};
  return Operator(group_by);
}
Operator LogicalAggregateAndGroupBy::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> &columns,
    std::vector<AnnotatedExpression> &having) {
  LogicalAggregateAndGroupBy *group_by = new LogicalAggregateAndGroupBy;
  group_by->columns = move(columns);
  group_by->having = move(having);
  return Operator(group_by);
}

bool LogicalAggregateAndGroupBy::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::LogicalAggregateAndGroupBy) return false;
  const LogicalAggregateAndGroupBy &r =
      *static_cast<const LogicalAggregateAndGroupBy *>(&node);
  if (having.size() != r.having.size() || columns.size() != r.columns.size())
    return false;
  for (size_t i = 0; i < having.size(); i++) {
    if (!having[i].expr->
        ExactlyEquals(*r.having[i].expr.get()))
      return false;
  }
  return expression::ExpressionUtil::EqualExpressions(columns, r.columns);
}

hash_t LogicalAggregateAndGroupBy::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : having) hash = HashUtil::SumHashes(hash, pred.expr->Hash());
  for (auto expr : columns) hash = HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
Operator LogicalInsert::make(
    std::shared_ptr<catalog::TableCatalogObject> target_table, const std::vector<std::string> *columns,
    const std::vector<std::vector<
        std::unique_ptr<peloton::expression::AbstractExpression>>> *values) {
  LogicalInsert *insert_op = new LogicalInsert;
  insert_op->target_table = target_table;
  insert_op->columns = columns;
  insert_op->values = values;
  return Operator(insert_op);
}

Operator LogicalInsertSelect::make(std::shared_ptr<catalog::TableCatalogObject> target_table) {
  LogicalInsertSelect *insert_op = new LogicalInsertSelect;
  insert_op->target_table = target_table;
  return Operator(insert_op);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
Operator LogicalDelete::make(std::shared_ptr<catalog::TableCatalogObject> target_table) {
  LogicalDelete *delete_op = new LogicalDelete;
  delete_op->target_table = target_table;
  return Operator(delete_op);
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
Operator LogicalUpdate::make(
    std::shared_ptr<catalog::TableCatalogObject> target_table,
    const std::vector<std::unique_ptr<peloton::parser::UpdateClause>> *
        updates) {
  LogicalUpdate *update_op = new LogicalUpdate;
  update_op->target_table = target_table;
  update_op->updates = updates;
  return Operator(update_op);
}

//===--------------------------------------------------------------------===//
// Distinct
//===--------------------------------------------------------------------===//
Operator LogicalDistinct::make() {
  LogicalDistinct *distinct = new LogicalDistinct;
  return Operator(distinct);
}

//===--------------------------------------------------------------------===//
// Limit
//===--------------------------------------------------------------------===//
Operator LogicalLimit::make(int64_t offset, int64_t limit) {
  LogicalLimit *limit_op = new LogicalLimit;
  limit_op->offset = offset;
  limit_op->limit = limit;
  return Operator(limit_op);
}

//===--------------------------------------------------------------------===//
// DummyScan
//===--------------------------------------------------------------------===//
Operator DummyScan::make() {
  DummyScan *dummy = new DummyScan;
  return Operator(dummy);
}

//===--------------------------------------------------------------------===//
// SeqScan
//===--------------------------------------------------------------------===//
Operator PhysicalSeqScan::make(oid_t get_id, std::shared_ptr<catalog::TableCatalogObject> table,
                               std::string alias,
                               std::vector<AnnotatedExpression> predicates,
                               bool update) {
  assert(table != nullptr);
  PhysicalSeqScan *scan = new PhysicalSeqScan;
  scan->table_ = table;
  scan->table_alias = alias;
  scan->predicates = std::move(predicates);
  scan->is_for_update = update;
  scan->get_id = get_id;

  return Operator(scan);
}

bool PhysicalSeqScan::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::SeqScan) return false;
  const PhysicalSeqScan &node = *static_cast<const PhysicalSeqScan *>(&r);
  if (predicates.size() != node.predicates.size()) return false;
  for (size_t i = 0; i < predicates.size(); i++) {
    if (!predicates[i].expr->ExactlyEquals(*node.predicates[i].expr.get()))
      return false;
  }
  return get_id == node.get_id;
}

hash_t PhysicalSeqScan::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  for (auto &pred : predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
Operator PhysicalIndexScan::make(oid_t get_id, std::shared_ptr<catalog::TableCatalogObject> table,
                                 std::string alias,
                                 std::vector<AnnotatedExpression> predicates,
                                 bool update, oid_t index_id,
                                 std::vector<oid_t> key_column_id_list,
                                 std::vector<ExpressionType> expr_type_list,
                                 std::vector<type::Value> value_list) {
  assert(table != nullptr);
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

  return Operator(scan);
}

bool PhysicalIndexScan::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::IndexScan) return false;
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
  hash_t hash = BaseOperatorNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&index_id));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  for (auto &pred : predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// Query derived get
//===--------------------------------------------------------------------===//
Operator QueryDerivedScan::make(
    oid_t get_id, std::string alias,
    std::unordered_map<std::string,
                       std::shared_ptr<expression::AbstractExpression>>
        alias_to_expr_map) {
  QueryDerivedScan *get = new QueryDerivedScan;
  get->table_alias = alias;
  get->alias_to_expr_map = alias_to_expr_map;
  get->get_id = get_id;

  return Operator(get);
}

bool QueryDerivedScan::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::QueryDerivedScan) return false;
  const QueryDerivedScan &r = *static_cast<const QueryDerivedScan *>(&node);
  return get_id == r.get_id;
}

hash_t QueryDerivedScan::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&get_id));
  return hash;
}

//===--------------------------------------------------------------------===//
// OrderBy
//===--------------------------------------------------------------------===//
Operator PhysicalOrderBy::make() {
  PhysicalOrderBy *order_by = new PhysicalOrderBy;

  return Operator(order_by);
}

//===--------------------------------------------------------------------===//
// PhysicalLimit
//===--------------------------------------------------------------------===//
Operator PhysicalLimit::make(int64_t offset, int64_t limit) {
  PhysicalLimit *limit_op = new PhysicalLimit;
  limit_op->offset = offset;
  limit_op->limit = limit;
  return Operator(limit_op);
}

//===--------------------------------------------------------------------===//
// InnerNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalInnerNLJoin::make(
    std::vector<AnnotatedExpression> conditions,
    std::vector<std::unique_ptr<expression::AbstractExpression>>& left_keys,
    std::vector<std::unique_ptr<expression::AbstractExpression>>& right_keys) {
  PhysicalInnerNLJoin *join = new PhysicalInnerNLJoin();
  join->join_predicates = std::move(conditions);
  join->left_keys = std::move(left_keys);
  join->right_keys = std::move(right_keys);

  return Operator(join);
}

hash_t PhysicalInnerNLJoin::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &expr : left_keys)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool PhysicalInnerNLJoin::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::InnerNLJoin) return false;
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
    if (!join_predicates[i].expr->
        ExactlyEquals(*node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalLeftNLJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalLeftNLJoin *join = new PhysicalLeftNLJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalRightNLJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalRightNLJoin *join = new PhysicalRightNLJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalOuterNLJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalOuterNLJoin *join = new PhysicalOuterNLJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalInnerHashJoin::make(
    std::vector<AnnotatedExpression> conditions,
    std::vector<std::unique_ptr<expression::AbstractExpression>>& left_keys,
    std::vector<std::unique_ptr<expression::AbstractExpression>>& right_keys) {
  PhysicalInnerHashJoin *join = new PhysicalInnerHashJoin();
  join->join_predicates = std::move(conditions);
  join->left_keys = std::move(left_keys);
  join->right_keys = std::move(right_keys);
  return Operator(join);
}

hash_t PhysicalInnerHashJoin::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &expr : left_keys)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &expr : right_keys)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  for (auto &pred : join_predicates)
    hash = HashUtil::CombineHashes(hash, pred.expr->Hash());
  return hash;
}

bool PhysicalInnerHashJoin::operator==(const BaseOperatorNode &r) {
  if (r.type() != OpType::InnerHashJoin) return false;
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
    if (!join_predicates[i].expr->
        ExactlyEquals(*node.join_predicates[i].expr.get()))
      return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalLeftHashJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalLeftHashJoin *join = new PhysicalLeftHashJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalRightHashJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalRightHashJoin *join = new PhysicalRightHashJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalOuterHashJoin::make(
    std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalOuterHashJoin *join = new PhysicalOuterHashJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// PhysicalInsert
//===--------------------------------------------------------------------===//
Operator PhysicalInsert::make(
    std::shared_ptr<catalog::TableCatalogObject> target_table, const std::vector<std::string> *columns,
    const std::vector<std::vector<
        std::unique_ptr<peloton::expression::AbstractExpression>>> *values) {
  PhysicalInsert *insert_op = new PhysicalInsert;
  insert_op->target_table = target_table;
  insert_op->columns = columns;
  insert_op->values = values;
  return Operator(insert_op);
}

//===--------------------------------------------------------------------===//
// PhysicalInsertSelect
//===--------------------------------------------------------------------===//
Operator PhysicalInsertSelect::make(std::shared_ptr<catalog::TableCatalogObject> target_table) {
  PhysicalInsertSelect *insert_op = new PhysicalInsertSelect;
  insert_op->target_table = target_table;
  return Operator(insert_op);
}

//===--------------------------------------------------------------------===//
// PhysicalDelete
//===--------------------------------------------------------------------===//
Operator PhysicalDelete::make(std::shared_ptr<catalog::TableCatalogObject> target_table) {
  PhysicalDelete *delete_op = new PhysicalDelete;
  delete_op->target_table = target_table;
  return Operator(delete_op);
}

//===--------------------------------------------------------------------===//
// PhysicalUpdate
//===--------------------------------------------------------------------===//
Operator PhysicalUpdate::make(
    std::shared_ptr<catalog::TableCatalogObject> target_table,
    const std::vector<std::unique_ptr<peloton::parser::UpdateClause>> *
        updates) {
  PhysicalUpdate *update = new PhysicalUpdate;
  update->target_table = target_table;
  update->updates = updates;
  return Operator(update);
}

//===--------------------------------------------------------------------===//
// PhysicalHashGroupBy
//===--------------------------------------------------------------------===//
Operator PhysicalHashGroupBy::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
    std::vector<AnnotatedExpression> having) {
  PhysicalHashGroupBy *agg = new PhysicalHashGroupBy;
  agg->columns = columns;
  agg->having = move(having);
  return Operator(agg);
}

bool PhysicalHashGroupBy::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::HashGroupBy) return false;
  const PhysicalHashGroupBy &r =
      *static_cast<const PhysicalHashGroupBy *>(&node);
  if (having.size() != r.having.size() || columns.size() != r.columns.size())
    return false;
  for (size_t i = 0; i < having.size(); i++) {
    if (!having[i].expr->
        ExactlyEquals(*r.having[i].expr.get()))
      return false;
  }
  return expression::ExpressionUtil::EqualExpressions(columns, r.columns);
}

hash_t PhysicalHashGroupBy::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : having) hash = HashUtil::SumHashes(hash, pred.expr->Hash());
  for (auto expr : columns) hash = HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// PhysicalSortGroupBy
//===--------------------------------------------------------------------===//
Operator PhysicalSortGroupBy::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
    std::vector<AnnotatedExpression> having) {
  PhysicalSortGroupBy *agg = new PhysicalSortGroupBy;
  agg->columns = std::move(columns);
  agg->having = move(having);
  return Operator(agg);
}

bool PhysicalSortGroupBy::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::SortGroupBy) return false;
  const PhysicalSortGroupBy &r =
      *static_cast<const PhysicalSortGroupBy *>(&node);
  if (having.size() != r.having.size() || columns.size() != r.columns.size())
    return false;
  for (size_t i = 0; i < having.size(); i++) {
    if (!having[i].expr->
        ExactlyEquals(*r.having[i].expr.get()))
      return false;
  }
  return expression::ExpressionUtil::EqualExpressions(columns, r.columns);
}

hash_t PhysicalSortGroupBy::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  for (auto &pred : having) hash = HashUtil::SumHashes(hash, pred.expr->Hash());
  for (auto expr : columns) hash = HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// PhysicalAggregate
//===--------------------------------------------------------------------===//
Operator PhysicalAggregate::make() {
  PhysicalAggregate *agg = new PhysicalAggregate;
  return Operator(agg);
}

//===--------------------------------------------------------------------===//
// Physical Hash
//===--------------------------------------------------------------------===//
Operator PhysicalDistinct::make() {
  PhysicalDistinct *hash = new PhysicalDistinct;
  return Operator(hash);
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
std::string OperatorNode<LogicalAggregateAndGroupBy>::name_ = "LogicalAggregateAndGroupBy";
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
std::string OperatorNode<DummyScan>::name_ = "DummyScan";
template <>
std::string OperatorNode<PhysicalSeqScan>::name_ = "PhysicalSeqScan";
template <>
std::string OperatorNode<PhysicalIndexScan>::name_ = "PhysicalIndexScan";
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

//===--------------------------------------------------------------------===//
template <>
OpType OperatorNode<LeafOperator>::type_ = OpType::Leaf;
template <>
OpType OperatorNode<LogicalGet>::type_ = OpType::Get;
template <>
OpType OperatorNode<LogicalQueryDerivedGet>::type_ =
    OpType::LogicalQueryDerivedGet;
template <>
OpType OperatorNode<LogicalFilter>::type_ = OpType::LogicalFilter;
template <>
OpType OperatorNode<LogicalProjection>::type_ = OpType::LogicalProjection;
template <>
OpType OperatorNode<LogicalMarkJoin>::type_ = OpType::LogicalMarkJoin;
template <>
OpType OperatorNode<LogicalSingleJoin>::type_ = OpType::LogicalSingleJoin;
template <>
OpType OperatorNode<LogicalDependentJoin>::type_ = OpType::LogicalDependentJoin;
template <>
OpType OperatorNode<LogicalInnerJoin>::type_ = OpType::InnerJoin;
template <>
OpType OperatorNode<LogicalLeftJoin>::type_ = OpType::LeftJoin;
template <>
OpType OperatorNode<LogicalRightJoin>::type_ = OpType::RightJoin;
template <>
OpType OperatorNode<LogicalOuterJoin>::type_ = OpType::OuterJoin;
template <>
OpType OperatorNode<LogicalSemiJoin>::type_ = OpType::SemiJoin;
template <>
OpType OperatorNode<LogicalAggregateAndGroupBy>::type_ = OpType::LogicalAggregateAndGroupBy;
template <>
OpType OperatorNode<LogicalInsert>::type_ = OpType::LogicalInsert;
template <>
OpType OperatorNode<LogicalInsertSelect>::type_ = OpType::LogicalInsertSelect;
template <>
OpType OperatorNode<LogicalUpdate>::type_ = OpType::LogicalUpdate;
template <>
OpType OperatorNode<LogicalDelete>::type_ = OpType::LogicalDelete;
template <>
OpType OperatorNode<LogicalDistinct>::type_ = OpType::LogicalDistinct;
template <>
OpType OperatorNode<LogicalLimit>::type_ = OpType::LogicalLimit;
template <>
OpType OperatorNode<DummyScan>::type_ = OpType::DummyScan;
template <>
OpType OperatorNode<PhysicalSeqScan>::type_ = OpType::SeqScan;
template <>
OpType OperatorNode<PhysicalIndexScan>::type_ = OpType::IndexScan;
template <>
OpType OperatorNode<QueryDerivedScan>::type_ = OpType::QueryDerivedScan;
template <>
OpType OperatorNode<PhysicalOrderBy>::type_ = OpType::OrderBy;
template <>
OpType OperatorNode<PhysicalDistinct>::type_ = OpType::Distinct;
template <>
OpType OperatorNode<PhysicalLimit>::type_ = OpType::PhysicalLimit;
template <>
OpType OperatorNode<PhysicalInnerNLJoin>::type_ = OpType::InnerNLJoin;
template <>
OpType OperatorNode<PhysicalLeftNLJoin>::type_ = OpType::LeftNLJoin;
template <>
OpType OperatorNode<PhysicalRightNLJoin>::type_ = OpType::RightNLJoin;
template <>
OpType OperatorNode<PhysicalOuterNLJoin>::type_ = OpType::OuterNLJoin;
template <>
OpType OperatorNode<PhysicalInnerHashJoin>::type_ = OpType::InnerHashJoin;
template <>
OpType OperatorNode<PhysicalLeftHashJoin>::type_ = OpType::LeftHashJoin;
template <>
OpType OperatorNode<PhysicalRightHashJoin>::type_ = OpType::RightHashJoin;
template <>
OpType OperatorNode<PhysicalOuterHashJoin>::type_ = OpType::OuterHashJoin;
template <>
OpType OperatorNode<PhysicalInsert>::type_ = OpType::Insert;
template <>
OpType OperatorNode<PhysicalInsertSelect>::type_ = OpType::InsertSelect;
template <>
OpType OperatorNode<PhysicalDelete>::type_ = OpType::Delete;
template <>
OpType OperatorNode<PhysicalUpdate>::type_ = OpType::Update;
template <>
OpType OperatorNode<PhysicalHashGroupBy>::type_ = OpType::HashGroupBy;
template <>
OpType OperatorNode<PhysicalSortGroupBy>::type_ = OpType::SortGroupBy;
template <>
OpType OperatorNode<PhysicalAggregate>::type_ = OpType::Aggregate;
//===--------------------------------------------------------------------===//
template <typename T>
bool OperatorNode<T>::IsLogical() const {
  return type_ < OpType::LogicalPhysicalDelimiter;
}

template <typename T>
bool OperatorNode<T>::IsPhysical() const {
  return type_ > OpType::LogicalPhysicalDelimiter;
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
