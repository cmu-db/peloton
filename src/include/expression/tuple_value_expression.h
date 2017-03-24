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
#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "planner/binding_context.h"

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
        tuple_idx_(-1),
        col_name_(col_name),
        ai_(nullptr) {}

  TupleValueExpression(std::string &&col_name, std::string &&table_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::Type::INVALID),
        value_idx_(-1),
        tuple_idx_(-1),
        table_name_(table_name),
        col_name_ (col_name),
        ai_(nullptr) { }

  TupleValueExpression(type::Type::TypeId type_id, const int tuple_idx,
                       const int value_idx)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type_id),
        value_idx_(value_idx),
        tuple_idx_(tuple_idx),
        ai_(nullptr) {}

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

  // Attribute binding
  void PerformBinding(const planner::BindingContext &binding_context) override {
    ai_ = binding_context.Find(GetColumnId());
    LOG_DEBUG("TVE Column ID %u binds to AI %p", GetColumnId(), ai_);
    PL_ASSERT(ai_ != nullptr);
  }

  // Return the attributes this expression uses
  void GetUsedAttributes(std::unordered_set<const planner::AttributeInfo *>
                         &attributes) const override {
    PL_ASSERT(GetAttributeRef() != nullptr);
    attributes.insert(GetAttributeRef());
  }

  AbstractExpression *Copy() const override {
    return new TupleValueExpression(*this);
  }

  const planner::AttributeInfo *GetAttributeRef() const { return ai_; }

  int GetColumnId() const { return value_idx_; }

  int GetTupleId() const { return tuple_idx_; }

  std::string GetTableName() const { return table_name_; }

  std::string GetColumnName() const { return col_name_; }

  void SetTableName(std::string table_alias) { table_name_ = table_alias; }

  // Binder stuff
  bool is_bound = false;
  std::tuple<oid_t, oid_t, oid_t> bound_obj_id;

  void SetBoundObjectId(std::tuple<oid_t, oid_t, oid_t> &col_pos_tuple) {
    bound_obj_id = col_pos_tuple;
  }

  virtual void Accept(SqlNodeVisitor *v) { v->Visit(this); }

 protected:
  TupleValueExpression(const TupleValueExpression &other)
      : AbstractExpression(other),
        value_idx_(other.value_idx_),
        tuple_idx_(other.tuple_idx_),
        table_name_(other.table_name_),
        col_name_(other.col_name_) {}

  int value_idx_;
  int tuple_idx_;
  std::string table_name_;
  std::string col_name_;

  const planner::AttributeInfo *ai_;
};

}  // namespace expression
}  // namespace peloton