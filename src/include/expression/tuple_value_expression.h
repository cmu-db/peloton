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

#include "expression/abstract_expression.h"

#include "common/internal_types.h"
#include "common/logger.h"
#include "common/sql_node_visitor.h"
#include "planner/binding_context.h"
#include "util/string_util.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// TupleValueExpression
//===----------------------------------------------------------------------===//

class TupleValueExpression : public AbstractExpression {
 public:
  TupleValueExpression(std::string &&col_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::TypeId::INVALID),
        value_idx_(-1),
        tuple_idx_(-1),
        col_name_(StringUtil::Lower(col_name)),
        ai_(nullptr) {}

  TupleValueExpression(std::string &&col_name, std::string &&table_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::TypeId::INVALID),
        value_idx_(-1),
        tuple_idx_(-1),
        table_name_(StringUtil::Lower(table_name)),
        col_name_(StringUtil::Lower(col_name)),
        ai_(nullptr) {}

  TupleValueExpression(type::TypeId type_id, const int tuple_idx,
                       const int value_idx)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type_id),
        value_idx_(value_idx),
        tuple_idx_(tuple_idx),
        ai_(nullptr) {}

  ~TupleValueExpression() {}

  virtual type::Value Evaluate(
      const AbstractTuple *tuple1, const AbstractTuple *tuple2,
      executor::ExecutorContext *context) const override;

  virtual void DeduceExpressionName() override {
    if (!alias.empty()) return;
    expr_name_ = col_name_;
  }

  // TODO: Delete this when TransformExpression is completely depracated
  void SetTupleValueExpressionParams(type::TypeId type_id, int value_idx,
                                     int tuple_idx) {
    return_value_type_ = type_id;
    value_idx_ = value_idx;
    tuple_idx_ = tuple_idx;
  }

  inline void SetValueType(type::TypeId type_id) {
    return_value_type_ = type_id;
  }

  inline void SetValueIdx(int value_idx, int tuple_idx = 0) {
    value_idx_ = value_idx;
    tuple_idx_ = tuple_idx;
  }

  /**
   * @brief Attribute binding
   * @param binding_contexts
   */
  void PerformBinding(const std::vector<const planner::BindingContext *> &
                          binding_contexts) override;

  // Return the attributes this expression uses
  void GetUsedAttributes(std::unordered_set<const planner::AttributeInfo *> &
                             attributes) const override {
    PL_ASSERT(GetAttributeRef() != nullptr);
    attributes.insert(GetAttributeRef());
  }

  AbstractExpression *Copy() const override {
    return new TupleValueExpression(*this);
  }

  virtual bool operator==(const AbstractExpression &rhs) const override {
    return ExactlyEquals(rhs);
  }

  virtual bool operator!=(const AbstractExpression &rhs) const override {
    return !(*this == rhs);
  }

  virtual bool ExactlyEquals(const AbstractExpression &rhs) const override {
    if (exp_type_ != rhs.GetExpressionType()) return false;

    auto &other = static_cast<const TupleValueExpression &>(rhs);
    // For query like SELECT A.id, B.id FROM test AS A, test AS B;
    // we need to know whether A.id is from A.id or B.id. In this case,
    // A.id and B.id have the same bound oids since they refer to the same table
    // but they have different table alias.
    if ((table_name_.empty() xor other.table_name_.empty()) ||
        col_name_.empty() xor other.col_name_.empty())
      return false;
    bool res = bound_obj_id_ == other.bound_obj_id_;
    if (!table_name_.empty() && !other.table_name_.empty())
      res = table_name_ == other.table_name_ && res;
    if (!col_name_.empty() && !other.col_name_.empty())
      res = col_name_ == other.col_name_ && res;
    return res;
  }

  virtual hash_t Hash() const override { return HashForExactMatch(); }

  virtual hash_t HashForExactMatch() const override;

  const planner::AttributeInfo *GetAttributeRef() const { return ai_; }

  int GetColumnId() const { return value_idx_; }

  int GetTupleId() const { return tuple_idx_; }

  std::string GetTableName() const { return table_name_; }

  std::string GetColumnName() const { return col_name_; }

  void SetTableName(std::string table_alias) { table_name_ = table_alias; }

  void SetColName(std::string col_name) { col_name_ = col_name; }

  bool GetIsBound() const { return is_bound_; }

  const std::tuple<oid_t, oid_t, oid_t> &GetBoundOid() const {
    return bound_obj_id_;
  }
  void SetBoundOid(oid_t db_id, oid_t table_id, oid_t col_id) {
    bound_obj_id_ = std::make_tuple(db_id, table_id, col_id);
    is_bound_ = true;
  }

  void SetBoundOid(std::tuple<oid_t, oid_t, oid_t> &bound_oid) {
    bound_obj_id_ = bound_oid;
    is_bound_ = true;
  }

  std::tuple<oid_t, oid_t, oid_t> GetBoundOid() {
    PL_ASSERT(is_bound_);
    return bound_obj_id_;
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  bool IsNullable() const override;

 protected:
  TupleValueExpression(const TupleValueExpression &other)
      : AbstractExpression(other),
        is_bound_(other.is_bound_),
        bound_obj_id_(other.bound_obj_id_),
        value_idx_(other.value_idx_),
        tuple_idx_(other.tuple_idx_),
        table_name_(other.table_name_),
        col_name_(other.col_name_) {}

  // Bound flag
  bool is_bound_ = false;
  // Bound ids. Init to INVALID_OID
  std::tuple<oid_t, oid_t, oid_t> bound_obj_id_ =
      std::make_tuple(INVALID_OID, INVALID_OID, INVALID_OID);
  int value_idx_;
  int tuple_idx_;
  std::string table_name_;
  std::string col_name_;

  const planner::AttributeInfo *ai_;
};

}  // namespace expression
}  // namespace peloton
