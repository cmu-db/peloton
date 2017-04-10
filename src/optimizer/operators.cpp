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
Operator LogicalGet::make(storage::DataTable *table, std::string alias) {
  LogicalGet *get = new LogicalGet;
  get->table = table;
  get->table_alias = alias;
  util::to_lower_string(get->table_alias);
  return Operator(get);
}

bool LogicalGet::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::Get) return false;
  const LogicalGet &r = *static_cast<const LogicalGet *>(&node);
  //  if (table->GetOid() != r.table->GetOid()) return false;
  //
  //  return true;
  return table_alias == r.table_alias;
}

hash_t LogicalGet::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  oid_t table_oid = table->GetOid();
  hash = util::CombineHashes(hash, util::Hash<oid_t>(&table_oid));
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
  join->condition = condition;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
Operator LogicalLeftJoin::make(expression::AbstractExpression *condition) {
  LogicalLeftJoin *join = new LogicalLeftJoin;
  join->condition = condition;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
Operator LogicalRightJoin::make(expression::AbstractExpression *condition) {
  LogicalRightJoin *join = new LogicalRightJoin;
  join->condition = condition;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
Operator LogicalOuterJoin::make(expression::AbstractExpression *condition) {
  LogicalOuterJoin *join = new LogicalOuterJoin;
  join->condition = condition;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
Operator LogicalSemiJoin::make(expression::AbstractExpression *condition) {
  LogicalSemiJoin *join = new LogicalSemiJoin;
  join->condition = condition;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//
Operator LogicalAggregate::make(
    std::vector<expression::AbstractExpression *> *columns,
    expression::AbstractExpression *having) {
  LogicalAggregate *agg = new LogicalAggregate;
  agg->columns = columns;
  agg->having = having;
  return Operator(agg);
}

//===--------------------------------------------------------------------===//
// Limit
//===--------------------------------------------------------------------===//
Operator LogicalLimit::make(int64_t limit, int64_t offset) {
  LogicalLimit *limit_op = new LogicalLimit;
  limit_op->limit = limit;
  limit_op->offset = offset;
  return Operator(limit_op);
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
Operator LogicalUpdate::make(const parser::UpdateStatement *update_stmt) {
  LogicalUpdate *update_op = new LogicalUpdate;
  update_op->update_stmt = update_stmt;
  return Operator(update_op);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
Operator PhysicalScan::make(storage::DataTable *table) {
  PhysicalScan *scan = new PhysicalScan;
  scan->table_ = table;
  return Operator(scan);
}

bool PhysicalScan::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::Scan) return false;
  const PhysicalScan &r = *static_cast<const PhysicalScan *>(&node);
  if (table_->GetOid() != r.table_->GetOid()) return false;
  return true;
}

hash_t PhysicalScan::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  oid_t table_oid = table_->GetOid();
  hash = util::CombineHashes(hash, util::Hash<oid_t>(&table_oid));
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
Operator PhysicalOrderBy::make(
    std::vector<expression::TupleValueExpression *> &sort_columns,
    std::vector<bool> &sort_ascending) {
  PhysicalOrderBy *order_by = new PhysicalOrderBy;
  order_by->sort_columns = sort_columns;
  order_by->sort_ascending = sort_ascending;
  return Operator(order_by);
}

//===--------------------------------------------------------------------===//
// Physical Limit
//===--------------------------------------------------------------------===//
Operator PhysicalLimit::make(int64_t limit, int64_t offset) {
  PhysicalLimit *limit_op = new PhysicalLimit;
  limit_op->limit = limit;
  limit_op->offset = offset;
  return Operator(limit_op);
}

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
Operator PhysicalInnerNLJoin::make() {
  PhysicalInnerNLJoin *join = new PhysicalInnerNLJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalLeftNLJoin::make() {
  PhysicalLeftNLJoin *join = new PhysicalLeftNLJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalRightNLJoin::make() {
  PhysicalRightNLJoin *join = new PhysicalRightNLJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalOuterNLJoin::make() {
  PhysicalOuterNLJoin *join = new PhysicalOuterNLJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalInnerHashJoin::make() {
  PhysicalInnerHashJoin *join = new PhysicalInnerHashJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalLeftHashJoin::make() {
  PhysicalLeftHashJoin *join = new PhysicalLeftHashJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalRightHashJoin::make() {
  PhysicalRightHashJoin *join = new PhysicalRightHashJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalOuterHashJoin::make() {
  PhysicalOuterHashJoin *join = new PhysicalOuterHashJoin;
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
Operator PhysicalUpdate::make(const parser::UpdateStatement *update_stmt) {
  PhysicalUpdate *update = new PhysicalUpdate;
  update->update_stmt = update_stmt;
  return Operator(update);
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
void OperatorNode<LogicalLimit>::Accept(
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
std::string OperatorNode<LogicalLimit>::name_ = "LogicalLimit";
template <>
std::string OperatorNode<LogicalInsert>::name_ = "LogicalInsert";
template <>
std::string OperatorNode<LogicalUpdate>::name_ = "LogicalUpdate";
template <>
std::string OperatorNode<LogicalDelete>::name_ = "LogicalDelete";
template <>
std::string OperatorNode<PhysicalScan>::name_ = "PhysicalScan";
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
OpType OperatorNode<LogicalAggregate>::type_ = OpType::Aggregate;
template <>
OpType OperatorNode<LogicalLimit>::type_ = OpType::Limit;
template <>
OpType OperatorNode<LogicalInsert>::type_ = OpType::LogicalInsert;
template <>
OpType OperatorNode<LogicalUpdate>::type_ = OpType::LogicalUpdate;
template <>
OpType OperatorNode<LogicalDelete>::type_ = OpType::LogicalDelete;
template <>
OpType OperatorNode<PhysicalScan>::type_ = OpType::Scan;
template <>
OpType OperatorNode<PhysicalProject>::type_ = OpType::Project;
template <>
OpType OperatorNode<PhysicalOrderBy>::type_ = OpType::OrderBy;
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
