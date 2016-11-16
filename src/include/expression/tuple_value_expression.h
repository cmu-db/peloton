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
#include "common/abstract_tuple.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// TupleValueExpression
//===----------------------------------------------------------------------===//

using namespace peloton::common;

class TupleValueExpression: public AbstractExpression {
public:

  TupleValueExpression(std::string &&col_name) :
      AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE, Type::INVALID), value_idx_(
          -1), tuple_idx_(-1) {
    col_name_ = col_name;
  }

  TupleValueExpression(std::string &&col_name, std::string &&table_name) :
      AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE, Type::INVALID), value_idx_(
          -1), tuple_idx_(-1) {
    table_name_ = table_name;
    col_name_ = col_name;
  }

  TupleValueExpression(Type::TypeId type_id, const int tuple_idx,
      const int value_idx) :
      AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE, type_id), value_idx_(
          value_idx), tuple_idx_(tuple_idx) {
  }

  ~TupleValueExpression() {
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
  UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    if (tuple_idx_ == 0) {
      assert(tuple1 != nullptr);
      return (tuple1->GetValue(value_idx_));
    } else {
      assert(tuple2 != nullptr);
      return (tuple2->GetValue(value_idx_));
    }
  }

  void SetTupleValueExpressionParams(Type::TypeId type_id, int value_idx,
      int tuple_idx) {
    value_type_ = type_id;
    value_idx_ = value_idx;
    tuple_idx_ = tuple_idx;
  }

  AbstractExpression *Copy() const override {
    return new TupleValueExpression(*this);
  }

  int GetColumnId() const {
    return value_idx_;
  }

  std::string table_name_;
  std::string col_name_;
  int value_idx_;
  int tuple_idx_;

protected:
  TupleValueExpression(const TupleValueExpression& other) :
      AbstractExpression(other), table_name_(other.table_name_), col_name_(other.col_name_), value_idx_(other.value_idx_), tuple_idx_(
          other.tuple_idx_) {
  }

};

}  // End expression namespace
}  // End peloton namespace
