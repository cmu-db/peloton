//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_value_expression.h
//
// Identification: src/include/expression/tuple_value_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/abstract_tuple.h"
#include "expression/abstract_expression.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// TupleValueExpression
//===----------------------------------------------------------------------===//

class TupleValueExpression : public AbstractExpression {
 public:
  TupleValueExpression(std::string &&col_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::Type::INVALID),
        value_idx_(-1),
        tuple_idx_(-1) {
    col_name_ = col_name;
  }

  TupleValueExpression(std::string &&col_name, std::string &&table_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::Type::INVALID),
        value_idx_(-1),
        tuple_idx_(-1) {
    table_name_ = table_name;
    col_name_ = col_name;
  }

  TupleValueExpression(type::Type::TypeId type_id, const int tuple_idx,
                       const int value_idx)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type_id),
        value_idx_(value_idx),
        tuple_idx_(tuple_idx) {}

  ~TupleValueExpression() {}

  type::Value Evaluate(
      const AbstractTuple *tuple1, const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    if (tuple_idx_ == 0) {
      PL_ASSERT(tuple1 != nullptr);
      return (tuple1->GetValue(value_idx_));
    } else {
      PL_ASSERT(tuple2 != nullptr);
      return (tuple2->GetValue(value_idx_));
    }
  }

  void SetTupleValueExpressionParams(type::Type::TypeId type_id, int value_idx,
                                     int tuple_idx) {
    return_value_type_ = type_id;
    value_idx_ = value_idx;
    tuple_idx_ = tuple_idx;
  }

  AbstractExpression *Copy() const override {
    return new TupleValueExpression(*this);
  }

  virtual bool Equals(AbstractExpression *expr) override {
    if (!AbstractExpression::Equals(expr)) return false;
    auto tup_expr = (TupleValueExpression *) expr;
    return bound_obj_id_ == tup_expr->bound_obj_id_;
  }

  virtual hash_t Hash() const {
    hash_t hash = peloton::Hash(&exp_type_);
    hash = CombineHashes(hash, peloton::Hash(&(std::get<0>(bound_obj_id_))));
    hash = CombineHashes(hash, peloton::Hash(&(std::get<1>(bound_obj_id_))));
    hash = CombineHashes(hash, peloton::Hash(&(std::get<2>(bound_obj_id_))));
    return hash;
  }

  int GetColumnId() const { return value_idx_; }

  int GetTupleId() const { return tuple_idx_; }

  std::string GetTableName() const { return table_name_; }

  std::string GetColumnName() const { return col_name_; }

  void SetTableName(std::string table_alias) { table_name_ = table_alias; }

  bool GetIsBound() const { return is_bound_; }

  const std::tuple<oid_t, oid_t, oid_t> &GetBoundOid() const {
    return bound_obj_id_;
  }

  void SetIsBound() { is_bound_ = true; }

  void SetBoundOid(std::tuple<oid_t, oid_t, oid_t> &bound_oid) {
    bound_obj_id_ = bound_oid;
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 protected:
  TupleValueExpression(const TupleValueExpression &other)
      : AbstractExpression(other),
        is_bound_(other.is_bound_),
        bound_obj_id_(other.bound_obj_id_),
        value_idx_(other.value_idx_),
        tuple_idx_(other.tuple_idx_),
        table_name_(other.table_name_),
        col_name_(other.col_name_) {}

  // Binder stuff
  bool is_bound_ = false;
  std::tuple<oid_t, oid_t, oid_t> bound_obj_id_;
  int value_idx_;
  int tuple_idx_;
  std::string table_name_;
  std::string col_name_;
};

}  // End expression namespace
}  // End peloton namespace
