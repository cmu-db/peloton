//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_value_expression.h
//
// Identification: src/backend/expression/tuple_value_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"
#include "backend/storage/tuple.h"

#include <string>
#include <sstream>

namespace peloton {
namespace expression {

class TupleValueExpression : public AbstractExpression {
 public:
  TupleValueExpression(ValueType type, const int tuple_idx, const int value_idx)
      : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE, type),
        tuple_idx_(tuple_idx),
        value_idx_(value_idx) {
    LOG_TRACE(
        "OptimizedTupleValueExpression %d using tuple index %d and value index "
        "%d",
        GetValueType(), tuple_idx_, value_idx_);
  };

  virtual Value Evaluate(const AbstractTuple *tuple1,
                         const AbstractTuple *tuple2,
                         __attribute__((unused))
                         executor::ExecutorContext *context) const override {
    if (tuple_idx_ == 0) {
      ALWAYS_ASSERT(tuple1);
      if (!tuple1) {
        throw Exception(
            "TupleValueExpression::"
            "Evaluate:"
            " Couldn't find tuple 1 (possible index scan planning error)");
      }
      return tuple1->GetValue(value_idx_);
    } else {
      ALWAYS_ASSERT(tuple2);
      if (!tuple2) {
        throw Exception(
            "TupleValueExpression::"
            "Evaluate:"
            " Couldn't find tuple 2 (possible index scan planning error)");
      }
      return tuple2->GetValue(value_idx_);
    }
  }

  std::string DebugInfo(const std::string &spacer) const override {
    std::ostringstream buffer;
    buffer << spacer << "Optimized Column Reference[" << tuple_idx_ << ", "
           << value_idx_ << "]\n";
    return (buffer.str());
  }

  int GetColumnId() const { return this->value_idx_; }

  int GetTupleIdx() const { return this->tuple_idx_; }

  AbstractExpression *Copy() const override {
    return new TupleValueExpression(GetValueType(), tuple_idx_, value_idx_);
  }

 protected:
  const int tuple_idx_;  // which tuple. defaults to tuple1
  const int value_idx_;  // which (offset) column of the tuple
};

}  // End expression namespace
}  // End peloton namespace
