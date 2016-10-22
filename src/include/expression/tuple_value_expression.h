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

class TupleValueExpression : public AbstractExpression {
 public:
  TupleValueExpression(Type::TypeId type_id, const int tuple_idx,
                       const int value_idx)
    : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE, type_id),
      value_idx_(value_idx), tuple_idx_(tuple_idx) {}

  ~TupleValueExpression() {}

  Value Evaluate(const AbstractTuple *tuple1,
                                  const AbstractTuple *tuple2,
          UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    if (tuple_idx_ == 0) {
      assert(tuple1 != nullptr);
      return (tuple1->GetValue(value_idx_));
    }
    else {
      assert(tuple2 != nullptr);
      return (tuple2->GetValue(value_idx_));
    }
  }

  AbstractExpression *Copy() const override {
    return new TupleValueExpression(value_type_, tuple_idx_, value_idx_);
  }

  int GetColumnId() const { return value_idx_; }

 protected:
  int value_idx_;
  int tuple_idx_;
};

}  // End expression namespace
}  // End peloton namespace
