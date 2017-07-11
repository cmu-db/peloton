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
#include "storage/data_table.h"
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
Operator LogicalGet::make(storage::DataTable *table, std::string alias,
                          bool update) {
  LogicalGet *get = new LogicalGet;
  get->table = table;
  get->table_alias = alias;
  get->is_for_update = update;
  util::to_lower_string(get->table_alias);
  return Operator(get);
}

bool LogicalGet::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::Get) return false;
  const LogicalGet &r = *static_cast<const LogicalGet *>(&node);
  return table_alias == r.table_alias && is_for_update == r.is_for_update;
}

hash_t LogicalGet::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  if (table == nullptr)
    return hash;
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(table_alias.c_str(), table_alias.length()));
  return hash;
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
Operator LogicalFilter::make() {
  LogicalFilter *select = new LogicalFilter;
  return Operator(select);
}

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
Operator LogicalInnerJoin::make(expression::AbstractExpression *condition) {
  LogicalInnerJoin *join = new LogicalInnerJoin;
  join->join_predicate = 
      std::shared_ptr<expression::AbstractExpression>(condition);
  return Operator(join);
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
Operator LogicalAggregate::make() {
  LogicalAggregate *agg = new LogicalAggregate;
  return Operator(agg);
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
Operator LogicalGroupBy::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
    expression::AbstractExpression *having) {
  LogicalGroupBy *group_by = new LogicalGroupBy;
  group_by->columns = move(columns);
  group_by->having = having;
  return Operator(group_by);
}

//===--------------------------------------------------------------------===//
// Insert
//===--------------------------------------------------------------------===//
Operator LogicalInsert::make(
    storage::DataTable *target_table, std::vector<char *> *columns,
    std::vector<std::vector<peloton::expression::AbstractExpression *> *> *
        values) {
  LogicalInsert *insert_op = new LogicalInsert;
  insert_op->target_table = target_table;
  insert_op->columns = columns;
  insert_op->values = values;
  return Operator(insert_op);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
Operator LogicalDelete::make(storage::DataTable *target_table) {
  LogicalDelete *delete_op = new LogicalDelete;
  delete_op->target_table = target_table;
  return Operator(delete_op);
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
Operator LogicalUpdate::make(
    storage::DataTable *target_table,
    std::vector<peloton::parser::UpdateClause *> updates) {
  LogicalUpdate *update_op = new LogicalUpdate;
  update_op->target_table = target_table;
  update_op->updates = updates;
  return Operator(update_op);
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
Operator PhysicalSeqScan::make(
    storage::DataTable *table, std::string alias, bool update) {
  assert(table != nullptr);
  PhysicalSeqScan *scan = new PhysicalSeqScan;
  scan->table_ = table;
  scan->table_alias = alias;
  scan->is_for_update = update;
  return Operator(scan);
}

bool PhysicalSeqScan::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::SeqScan) return false;
  const PhysicalSeqScan &r = *static_cast<const PhysicalSeqScan *>(&node);
  return table_alias == r.table_alias && is_for_update == r.is_for_update;
}

hash_t PhysicalSeqScan::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(table_alias.c_str(), table_alias.length()));
  return hash;
}

//===--------------------------------------------------------------------===//
// IndexScan
//===--------------------------------------------------------------------===//
Operator PhysicalIndexScan::make(
    storage::DataTable *table, std::string alias, bool update) {
  assert(table != nullptr);
  PhysicalIndexScan *scan = new PhysicalIndexScan;
  scan->table_ = table;
  scan->is_for_update = update;
  scan->table_alias = alias;
  return Operator(scan);
}

bool PhysicalIndexScan::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::IndexScan) return false;
  const PhysicalIndexScan &r = *static_cast<const PhysicalIndexScan *>(&node);
  return table_alias == r.table_alias && is_for_update == r.is_for_update;
}

hash_t PhysicalIndexScan::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(table_alias.c_str(), table_alias.length()));
  return hash;
}

//===--------------------------------------------------------------------===//
// Project
//===--------------------------------------------------------------------===//
Operator PhysicalProject::make() {
  PhysicalProject *project = new PhysicalProject;
  return Operator(project);
}

//===--------------------------------------------------------------------===//
// OrderBy
//===--------------------------------------------------------------------===//
Operator PhysicalOrderBy::make() {
  PhysicalOrderBy *order_by = new PhysicalOrderBy;

  return Operator(order_by);
}

//===--------------------------------------------------------------------===//
// Physical Limit
//===--------------------------------------------------------------------===//
Operator PhysicalLimit::make() { return Operator(new PhysicalLimit); }

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
Operator PhysicalFilter::make() {
  PhysicalFilter *filter = new PhysicalFilter;
  return Operator(filter);
}

//===--------------------------------------------------------------------===//
// InnerNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalInnerNLJoin::make(std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalInnerNLJoin *join = new PhysicalInnerNLJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalLeftNLJoin::make(std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalLeftNLJoin *join = new PhysicalLeftNLJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalRightNLJoin::make(std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalRightNLJoin *join = new PhysicalRightNLJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalOuterNLJoin::make(std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalOuterNLJoin *join = new PhysicalOuterNLJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalInnerHashJoin::make(std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalInnerHashJoin *join = new PhysicalInnerHashJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalLeftHashJoin::make(std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalLeftHashJoin *join = new PhysicalLeftHashJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalRightHashJoin::make(std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalRightHashJoin *join = new PhysicalRightHashJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalOuterHashJoin::make(std::shared_ptr<expression::AbstractExpression> join_predicate) {
  PhysicalOuterHashJoin *join = new PhysicalOuterHashJoin();
  join->join_predicate = join_predicate;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// PhysicalInsert
//===--------------------------------------------------------------------===//
Operator PhysicalInsert::make(
    storage::DataTable *target_table, std::vector<char *> *columns,
    std::vector<std::vector<peloton::expression::AbstractExpression *> *> *
        values) {
  PhysicalInsert *insert_op = new PhysicalInsert;
  insert_op->target_table = target_table;
  insert_op->columns = columns;
  insert_op->values = values;
  return Operator(insert_op);
}

//===--------------------------------------------------------------------===//
// PhysicalDelete
//===--------------------------------------------------------------------===//
Operator PhysicalDelete::make(storage::DataTable *target_table) {
  PhysicalDelete *delete_op = new PhysicalDelete;
  delete_op->target_table = target_table;
  return Operator(delete_op);
}

//===--------------------------------------------------------------------===//
// PhysicalUpdate
//===--------------------------------------------------------------------===//
Operator PhysicalUpdate::make(
    storage::DataTable *target_table,
    std::vector<peloton::parser::UpdateClause *> updates) {
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
    expression::AbstractExpression *having) {
  PhysicalHashGroupBy *agg = new PhysicalHashGroupBy;
  agg->columns = std::move(columns);
  agg->having = having;
  return Operator(agg);
}

bool PhysicalHashGroupBy::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::HashGroupBy) return false;
  const PhysicalHashGroupBy &r = *static_cast<const PhysicalHashGroupBy *>(&node);
  if ((having == nullptr && r.having != nullptr) || (r.having == nullptr && having != nullptr))
    return false;
  return expression::ExpressionUtil::EqualExpressions(columns, r.columns);
}

hash_t PhysicalHashGroupBy::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  if (having != nullptr)
    hash = HashUtil::SumHashes(hash, having->Hash());
  for (auto expr : columns)
    hash = HashUtil::SumHashes(hash, expr->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// PhysicalSortGroupBy
//===--------------------------------------------------------------------===//
Operator PhysicalSortGroupBy::make(
    std::vector<std::shared_ptr<expression::AbstractExpression>> columns,
    expression::AbstractExpression *having) {
  PhysicalSortGroupBy *agg = new PhysicalSortGroupBy;
  agg->columns = std::move(columns);
  agg->having = having;
  return Operator(agg);
}

bool PhysicalSortGroupBy::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::SortGroupBy) return false;
  const PhysicalSortGroupBy &r = *static_cast<const PhysicalSortGroupBy *>(&node);
  if ((having == nullptr && r.having != nullptr) || (r.having == nullptr && having != nullptr))
    return false;
  return expression::ExpressionUtil::EqualExpressions(columns, r.columns);
}

hash_t PhysicalSortGroupBy::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  if (having != nullptr)
    hash = HashUtil::SumHashes(hash, having->Hash());
  for (auto expr : columns)
    hash = HashUtil::SumHashes(hash, expr->Hash());
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

// We don't need to visit logical operators for driving child properties,
// costing plans, etc.
template <>
void OperatorNode<LeafOperator>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalGet>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalFilter>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalInnerJoin>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalLeftJoin>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalRightJoin>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalOuterJoin>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalAggregate>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalGroupBy>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalSemiJoin>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalInsert>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalDelete>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}
template <>
void OperatorNode<LogicalUpdate>::Accept(
    UNUSED_ATTRIBUTE OperatorVisitor *v) const {}

//===--------------------------------------------------------------------===//
template <>
std::string OperatorNode<LeafOperator>::name_ = "LeafOperator";
template <>
std::string OperatorNode<LogicalGet>::name_ = "LogicalGet";
template <>
std::string OperatorNode<LogicalFilter>::name_ = "LogicalFilter";
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
std::string OperatorNode<LogicalAggregate>::name_ = "LogicalAggregate";
template <>
std::string OperatorNode<LogicalGroupBy>::name_ = "LogicalGroupBy";
template <>
std::string OperatorNode<LogicalInsert>::name_ = "LogicalInsert";
template <>
std::string OperatorNode<LogicalUpdate>::name_ = "LogicalUpdate";
template <>
std::string OperatorNode<LogicalDelete>::name_ = "LogicalDelete";
template <>
std::string OperatorNode<DummyScan>::name_ = "DummyScan";
template <>
std::string OperatorNode<PhysicalSeqScan>::name_ = "PhysicalSeqScan";
template <>
std::string OperatorNode<PhysicalIndexScan>::name_ = "PhysicalIndexScan";
template <>
std::string OperatorNode<PhysicalProject>::name_ = "PhysicalProject";
template <>
std::string OperatorNode<PhysicalOrderBy>::name_ = "PhysicalOrderBy";
template <>
std::string OperatorNode<PhysicalLimit>::name_ = "PhysicalLimit";
template <>
std::string OperatorNode<PhysicalFilter>::name_ = "PhysicalFilter";
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
OpType OperatorNode<LogicalFilter>::type_ = OpType::LogicalFilter;
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
OpType OperatorNode<LogicalAggregate>::type_ = OpType::LogicalAggregate;
template <>
OpType OperatorNode<LogicalGroupBy>::type_ = OpType::LogicalGroupBy;
template <>
OpType OperatorNode<LogicalInsert>::type_ = OpType::LogicalInsert;
template <>
OpType OperatorNode<LogicalUpdate>::type_ = OpType::LogicalUpdate;
template <>
OpType OperatorNode<LogicalDelete>::type_ = OpType::LogicalDelete;
template <>
OpType OperatorNode<DummyScan>::type_ = OpType::DummyScan;
template <>
OpType OperatorNode<PhysicalSeqScan>::type_ = OpType::SeqScan;
template <>
OpType OperatorNode<PhysicalIndexScan>::type_ = OpType::IndexScan;
template <>
OpType OperatorNode<PhysicalProject>::type_ = OpType::Project;
template <>
OpType OperatorNode<PhysicalOrderBy>::type_ = OpType::OrderBy;
template <>
OpType OperatorNode<PhysicalDistinct>::type_ = OpType::Distinct;
template <>
OpType OperatorNode<PhysicalLimit>::type_ = OpType::PhysicalLimit;
template <>
OpType OperatorNode<PhysicalFilter>::type_ = OpType::Filter;
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

} /* namespace optimizer */
} /* namespace peloton */
