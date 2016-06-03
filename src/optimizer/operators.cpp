//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_operators.cpp
//
// Identification: src/optimizer/logical_operators.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/operators.h"
#include "optimizer/operator_visitor.h"

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
Operator LogicalGet::make(storage::DataTable *table,
                          std::vector<Column *> cols)
{
  LogicalGet *get = new LogicalGet;
  get->table = table;
  get->columns = cols;
  return Operator(get);
}

bool LogicalGet::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::Get) return false;
  const LogicalGet &r = *static_cast<const LogicalGet*>(&node);
  if (table->GetOid() != r.table->GetOid()) return false;
  if (columns.size() != r.columns.size()) return false;

  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i] != r.columns[i]) return false;
  }
  return true;
}

hash_t LogicalGet::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  oid_t table_oid = table->GetOid();
  hash = util::CombineHashes(hash, util::Hash<oid_t>(&table_oid));
  for (Column *col : columns) {
    hash = util::CombineHashes(hash, col->Hash());
  }
  return hash;
}

//===--------------------------------------------------------------------===//
// Project
//===--------------------------------------------------------------------===//
Operator LogicalProject::make() {
  LogicalProject *project = new LogicalProject;
  return Operator(project);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
Operator LogicalSelect::make() {
  LogicalSelect *select = new LogicalSelect;
  return Operator(select);
}

//===--------------------------------------------------------------------===//
// InnerJoin
//===--------------------------------------------------------------------===//
Operator LogicalInnerJoin::make()
{
  LogicalInnerJoin *join = new LogicalInnerJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// LeftJoin
//===--------------------------------------------------------------------===//
Operator LogicalLeftJoin::make()
{
  LogicalLeftJoin *join = new LogicalLeftJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightJoin
//===--------------------------------------------------------------------===//
Operator LogicalRightJoin::make()
{
  LogicalRightJoin *join = new LogicalRightJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterJoin
//===--------------------------------------------------------------------===//
Operator LogicalOuterJoin::make()
{
  LogicalOuterJoin *join = new LogicalOuterJoin;
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
// Limit
//===--------------------------------------------------------------------===//
Operator LogicalLimit::make() {
  LogicalLimit *limit_op = new LogicalLimit;
  return Operator(limit_op);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
Operator PhysicalScan::make(storage::DataTable *table,
                            std::vector<Column *> cols)
{
  PhysicalScan *scan = new PhysicalScan;
  scan->table = table;
  scan->columns = cols;
  return Operator(scan);
}

bool PhysicalScan::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::Scan) return false;
  const PhysicalScan &r = *static_cast<const PhysicalScan *>(&node);
  if (table->GetOid() != r.table->GetOid()) return false;
  if (columns.size() != r.columns.size()) return false;

  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i] != r.columns[i]) return false;
  }
  return true;
}

hash_t PhysicalScan::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  oid_t table_oid = table->GetOid();
  hash = util::CombineHashes(hash, util::Hash<oid_t>(&table_oid));
  for (Column *col : columns) {
    hash = util::CombineHashes(hash, col->Hash());
  }
  return hash;
}

//===--------------------------------------------------------------------===//
// ComputeExprs
//===--------------------------------------------------------------------===//
Operator PhysicalComputeExprs::make() {
  PhysicalComputeExprs *compute = new PhysicalComputeExprs;
  return Operator(compute);
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
Operator PhysicalInnerNLJoin::make()
{
  PhysicalInnerNLJoin *join = new PhysicalInnerNLJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// LeftNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalLeftNLJoin::make()
{
  PhysicalLeftNLJoin *join = new PhysicalLeftNLJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalRightNLJoin::make()
{
  PhysicalRightNLJoin *join = new PhysicalRightNLJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterNLJoin
//===--------------------------------------------------------------------===//
Operator PhysicalOuterNLJoin::make()
{
  PhysicalOuterNLJoin *join = new PhysicalOuterNLJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// InnerHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalInnerHashJoin::make()
{
  PhysicalInnerHashJoin *join = new PhysicalInnerHashJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// LeftHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalLeftHashJoin::make()
{
  PhysicalLeftHashJoin *join = new PhysicalLeftHashJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// RightHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalRightHashJoin::make()
{
  PhysicalRightHashJoin *join = new PhysicalRightHashJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// OuterHashJoin
//===--------------------------------------------------------------------===//
Operator PhysicalOuterHashJoin::make()
{
  PhysicalOuterHashJoin *join = new PhysicalOuterHashJoin;
  return Operator(join);
}

//===--------------------------------------------------------------------===//
// Variable
//===--------------------------------------------------------------------===//
Operator ExprVariable::make(Column *column) {
  ExprVariable *var = new ExprVariable;
  var->column = column;
  return Operator(var);
}

bool ExprVariable::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::Variable) return false;
  const ExprVariable &r = *static_cast<const ExprVariable *>(&node);
  if (column != r.column) return false;
  return true;
}

hash_t ExprVariable::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = util::CombineHashes(hash, column->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//
// Constant
//===--------------------------------------------------------------------===//
Operator ExprConstant::make(Value value) {
  ExprConstant *constant = new ExprConstant;
  constant->value = value;
  return Operator(constant);
}

bool ExprConstant::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::Constant) return false;
  const ExprConstant &r = *static_cast<const ExprConstant *>(&node);
  if (value != r.value) return false;
  return true;
}

hash_t ExprConstant::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = util::CombineHashes(hash, value.MurmurHash3());
  return hash;
}

//===--------------------------------------------------------------------===//
// Compare
//===--------------------------------------------------------------------===//
Operator ExprCompare::make(ExpressionType type) {
  ExprCompare *cmp = new ExprCompare;
  cmp->expr_type = type;
  return Operator(cmp);
}

bool ExprCompare::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::Compare) return false;
  const ExprCompare &r = *static_cast<const ExprCompare *>(&node);
  if (expr_type != r.expr_type) return false;
  return true;
}

hash_t ExprCompare::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = util::CombineHashes(hash, util::Hash<ExpressionType>(&expr_type));
  return hash;
}

//===--------------------------------------------------------------------===//
// Boolean Operation
//===--------------------------------------------------------------------===//
Operator ExprBoolOp::make(BoolOpType type) {
  ExprBoolOp *bool_op = new ExprBoolOp;
  bool_op->bool_type = type;
  return Operator(bool_op);
}

bool ExprBoolOp::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::BoolOp) return false;
  const ExprBoolOp &r = *static_cast<const ExprBoolOp *>(&node);
  if (bool_type != r.bool_type) return false;
  return true;
}

hash_t ExprBoolOp::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = util::CombineHashes(hash, util::Hash<BoolOpType>(&bool_type));
  return hash;
}

//===--------------------------------------------------------------------===//
// Operation (e.g. +, -, string functions)
//===--------------------------------------------------------------------===//
Operator ExprOp::make(ExpressionType type, ValueType return_type) {
  ExprOp *op = new ExprOp;
  op->expr_type = type;
  op->return_type = return_type;
  return Operator(op);
}

bool ExprOp::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::Op) return false;
  const ExprOp &r = *static_cast<const ExprOp *>(&node);
  if (expr_type != r.expr_type) return false;
  if (return_type != r.return_type) return false;
  return true;
}

hash_t ExprOp::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = util::CombineHashes(hash, util::Hash<ExpressionType>(&expr_type));
  hash = util::CombineHashes(hash, util::Hash<ValueType>(&return_type));
  return hash;
}

//===--------------------------------------------------------------------===//
// ProjectList
//===--------------------------------------------------------------------===//
Operator ExprProjectList::make() {
  ExprProjectList *list = new ExprProjectList;
  return Operator(list);
}

//===--------------------------------------------------------------------===//
// ProjectColumn
//===--------------------------------------------------------------------===//
Operator ExprProjectColumn::make(Column *column) {
  ExprProjectColumn *project_col = new ExprProjectColumn;
  project_col->column = column;
  return Operator(project_col);
}

bool ExprProjectColumn::operator==(const BaseOperatorNode &node) {
  if (node.type() != OpType::ProjectColumn) return false;
  const ExprProjectColumn &r = *static_cast<const ExprProjectColumn *>(&node);
  if (column != r.column) return false;
  return true;
}

hash_t ExprProjectColumn::Hash() const {
  hash_t hash = BaseOperatorNode::Hash();
  hash = util::CombineHashes(hash, column->Hash());
  return hash;
}

//===--------------------------------------------------------------------===//

template<>
void OperatorNode<LeafOperator>::accept(OperatorVisitor *v) const {
  v->visit((const LeafOperator *)this);
}
template<>
void OperatorNode<LogicalGet>::accept(OperatorVisitor *v) const {
  v->visit((const LogicalGet *)this);
}
template<>
void OperatorNode<LogicalProject>::accept(OperatorVisitor *v) const {
  v->visit((const LogicalProject *)this);
}
template<>
void OperatorNode<LogicalSelect>::accept(OperatorVisitor *v) const {
  v->visit((const LogicalSelect *)this);
}
template<>
void OperatorNode<LogicalInnerJoin>::accept(OperatorVisitor *v) const {
  v->visit((const LogicalInnerJoin *)this);
}
template<>
void OperatorNode<LogicalLeftJoin>::accept(OperatorVisitor *v) const {
  v->visit((const LogicalLeftJoin *)this);
}
template<>
void OperatorNode<LogicalRightJoin>::accept(OperatorVisitor *v) const {
  v->visit((const LogicalRightJoin *)this);
}
template<>
void OperatorNode<LogicalOuterJoin>::accept(OperatorVisitor *v) const {
  v->visit((const LogicalOuterJoin *)this);
}
template<>
void OperatorNode<LogicalAggregate>::accept(OperatorVisitor *v) const {
  v->visit((const LogicalAggregate *)this);
}
template<>
void OperatorNode<LogicalLimit>::accept(OperatorVisitor *v) const {
  v->visit((const LogicalLimit *)this);
}
template<>
void OperatorNode<PhysicalScan>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalScan *)this);
}
template<>
void OperatorNode<PhysicalComputeExprs>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalComputeExprs *)this);
}
template<>
void OperatorNode<PhysicalFilter>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalFilter *)this);
}
template<>
void OperatorNode<PhysicalInnerNLJoin>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalInnerNLJoin *)this);
}
template<>
void OperatorNode<PhysicalLeftNLJoin>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalLeftNLJoin *)this);
}
template<>
void OperatorNode<PhysicalRightNLJoin>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalRightNLJoin *)this);
}
template<>
void OperatorNode<PhysicalOuterNLJoin>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalOuterNLJoin *)this);
}
template<>
void OperatorNode<PhysicalInnerHashJoin>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalInnerHashJoin *)this);
}
template<>
void OperatorNode<PhysicalLeftHashJoin>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalLeftHashJoin *)this);
}
template<>
void OperatorNode<PhysicalRightHashJoin>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalRightHashJoin *)this);
}
template<>
void OperatorNode<PhysicalOuterHashJoin>::accept(OperatorVisitor *v) const {
  v->visit((const PhysicalOuterHashJoin *)this);
}
template<>
void OperatorNode<ExprVariable>::accept(OperatorVisitor *v) const {
  v->visit((const ExprVariable *)this);
}
template<>
void OperatorNode<ExprConstant>::accept(OperatorVisitor *v) const {
  v->visit((const ExprConstant *)this);
}
template<>
void OperatorNode<ExprCompare>::accept(OperatorVisitor *v) const {
  v->visit((const ExprCompare *)this);
}
template<>
void OperatorNode<ExprBoolOp>::accept(OperatorVisitor *v) const {
  v->visit((const ExprBoolOp *)this);
}
template<>
void OperatorNode<ExprOp>::accept(OperatorVisitor *v) const {
  v->visit((const ExprOp *)this);
}
template<>
void OperatorNode<ExprProjectList>::accept(OperatorVisitor *v) const {
  v->visit((const ExprProjectList *)this);
}
template<>
void OperatorNode<ExprProjectColumn>::accept(OperatorVisitor *v) const {
  v->visit((const ExprProjectColumn *)this);
}

template<>
std::string OperatorNode<LeafOperator>::_name = "LeafOperator";
template<>
std::string OperatorNode<LogicalGet>::_name = "LogicalGet";
template<>
std::string OperatorNode<LogicalProject>::_name = "LogicalProject";
template<>
std::string OperatorNode<LogicalSelect>::_name = "LogicalSelect";
template<>
std::string OperatorNode<LogicalInnerJoin>::_name = "LogicalInnerJoin";
template<>
std::string OperatorNode<LogicalLeftJoin>::_name = "LogicalLeftJoin";
template<>
std::string OperatorNode<LogicalRightJoin>::_name = "LogicalRightJoin";
template<>
std::string OperatorNode<LogicalOuterJoin>::_name = "LogicalOuterJoin";
template<>
std::string OperatorNode<LogicalAggregate>::_name = "LogicalAggregate";
template<>
std::string OperatorNode<LogicalLimit>::_name = "LogicalLimit";
template<>
std::string OperatorNode<PhysicalScan>::_name = "PhysicalScan";
template<>
std::string OperatorNode<PhysicalComputeExprs>::_name = "PhysicalComputeExprs";
template<>
std::string OperatorNode<PhysicalFilter>::_name = "PhysicalFilter";
template<>
std::string OperatorNode<PhysicalInnerNLJoin>::_name = "PhysicalInnerNLJoin";
template<>
std::string OperatorNode<PhysicalLeftNLJoin>::_name = "PhysicalLeftNLJoin";
template<>
std::string OperatorNode<PhysicalRightNLJoin>::_name = "PhysicalRightNLJoin";
template<>
std::string OperatorNode<PhysicalOuterNLJoin>::_name = "PhysicalOuterNLJoin";
template<>
std::string OperatorNode<PhysicalInnerHashJoin>::_name = "PhysicalInnerHashJoin";
template<>
std::string OperatorNode<PhysicalLeftHashJoin>::_name = "PhysicalLeftHashJoin";
template<>
std::string OperatorNode<PhysicalRightHashJoin>::_name = "PhysicalRightHashJoin";
template<>
std::string OperatorNode<PhysicalOuterHashJoin>::_name = "PhysicalOuterHashJoin";
template<>
std::string OperatorNode<ExprVariable>::_name = "ExprVariable";
template<>
std::string OperatorNode<ExprConstant>::_name = "ExprConstant";
template<>
std::string OperatorNode<ExprCompare>::_name = "ExprCompare";
template<>
std::string OperatorNode<ExprBoolOp>::_name = "ExprBoolOp";
template<>
std::string OperatorNode<ExprOp>::_name = "ExprOp";
template<>
std::string OperatorNode<ExprProjectList>::_name = "ExprProjectList";
template<>
std::string OperatorNode<ExprProjectColumn>::_name = "ExprProjectColumn";

template<>
OpType OperatorNode<LeafOperator>::_type = OpType::Leaf;
template<>
OpType OperatorNode<LogicalGet>::_type = OpType::Get;
template<>
OpType OperatorNode<LogicalProject>::_type = OpType::Project;
template<>
OpType OperatorNode<LogicalSelect>::_type = OpType::Select;
template<>
OpType OperatorNode<LogicalInnerJoin>::_type = OpType::InnerJoin;
template<>
OpType OperatorNode<LogicalLeftJoin>::_type = OpType::LeftJoin;
template<>
OpType OperatorNode<LogicalRightJoin>::_type = OpType::RightJoin;
template<>
OpType OperatorNode<LogicalOuterJoin>::_type = OpType::OuterJoin;
template<>
OpType OperatorNode<LogicalAggregate>::_type = OpType::Aggregate;
template<>
OpType OperatorNode<LogicalLimit>::_type = OpType::Limit;
template<>
OpType OperatorNode<PhysicalScan>::_type = OpType::Scan;
template<>
OpType OperatorNode<PhysicalComputeExprs>::_type = OpType::ComputeExprs;
template<>
OpType OperatorNode<PhysicalFilter>::_type = OpType::Filter;
template<>
OpType OperatorNode<PhysicalInnerNLJoin>::_type = OpType::InnerNLJoin;
template<>
OpType OperatorNode<PhysicalLeftNLJoin>::_type = OpType::LeftNLJoin;
template<>
OpType OperatorNode<PhysicalRightNLJoin>::_type = OpType::RightNLJoin;
template<>
OpType OperatorNode<PhysicalOuterNLJoin>::_type = OpType::OuterNLJoin;
template<>
OpType OperatorNode<PhysicalInnerHashJoin>::_type = OpType::InnerHashJoin;
template<>
OpType OperatorNode<PhysicalLeftHashJoin>::_type = OpType::LeftHashJoin;
template<>
OpType OperatorNode<PhysicalRightHashJoin>::_type = OpType::RightHashJoin;
template<>
OpType OperatorNode<PhysicalOuterHashJoin>::_type = OpType::OuterHashJoin;
template<>
OpType OperatorNode<ExprVariable>::_type = OpType::Variable;
template<>
OpType OperatorNode<ExprConstant>::_type = OpType::Constant;
template<>
OpType OperatorNode<ExprCompare>::_type = OpType::Compare;
template<>
OpType OperatorNode<ExprBoolOp>::_type = OpType::BoolOp;
template<>
OpType OperatorNode<ExprOp>::_type = OpType::Op;
template<>
OpType OperatorNode<ExprProjectList>::_type = OpType::ProjectList;
template<>
OpType OperatorNode<ExprProjectColumn>::_type = OpType::ProjectColumn;

template<>
bool OperatorNode<LeafOperator>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<LogicalGet>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<LogicalProject>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<LogicalSelect>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<LogicalInnerJoin>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<LogicalLeftJoin>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<LogicalRightJoin>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<LogicalOuterJoin>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<LogicalAggregate>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<LogicalLimit>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalScan>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalComputeExprs>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalFilter>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalInnerNLJoin>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalLeftNLJoin>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalRightNLJoin>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalOuterNLJoin>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalInnerHashJoin>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalLeftHashJoin>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalRightHashJoin>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalOuterHashJoin>::IsLogical() const {
  return false;
}
template<>
bool OperatorNode<ExprVariable>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<ExprConstant>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<ExprCompare>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<ExprBoolOp>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<ExprOp>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<ExprProjectList>::IsLogical() const {
  return true;
}
template<>
bool OperatorNode<ExprProjectColumn>::IsLogical() const {
  return true;
}

template<>
bool OperatorNode<LeafOperator>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<LogicalGet>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<LogicalProject>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<LogicalSelect>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<LogicalInnerJoin>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<LogicalLeftJoin>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<LogicalRightJoin>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<LogicalOuterJoin>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<LogicalAggregate>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<LogicalLimit>::IsPhysical() const {
  return false;
}
template<>
bool OperatorNode<PhysicalScan>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalComputeExprs>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalFilter>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalInnerNLJoin>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalLeftNLJoin>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalRightNLJoin>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalOuterNLJoin>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalInnerHashJoin>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalLeftHashJoin>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalRightHashJoin>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<PhysicalOuterHashJoin>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<ExprVariable>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<ExprConstant>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<ExprCompare>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<ExprBoolOp>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<ExprOp>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<ExprProjectList>::IsPhysical() const {
  return true;
}
template<>
bool OperatorNode<ExprProjectColumn>::IsPhysical() const {
  return true;
}

} /* namespace optimizer */
} /* namespace peloton */
