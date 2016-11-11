//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_expression.h
//
// Identification: src/include/expression/function_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// OperatorExpression
//===----------------------------------------------------------------------===//

using namespace peloton::common;

class FunctionExpression: public AbstractExpression {
public:
  FunctionExpression(char * function_name) :
      AbstractExpression(EXPRESSION_TYPE_FUNCTION), func_ptr_(nullptr) {
  }

  FunctionExpression(Value (*func_ptr)(Value&), Type::TypeId type_id,
      AbstractExpression *left, AbstractExpression *right) :
      AbstractExpression(EXPRESSION_TYPE_FUNCTION, type_id, left, right), func_ptr_(
          func_ptr) {
  }

  void SetFunctionExpressionParameters(Value (*func_ptr_)(Value&),
      Type::TypeId val_type) {

  }

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
  UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
  UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    Value v1 = left_->Evaluate(tuple1, tuple2, context);
    return func_ptr_(v1);
  }

  AbstractExpression * Copy() const{
    return new FunctionExpression(*this);
  }

protected:
  FunctionExpression(const FunctionExpression& other) :
      AbstractExpression(other), func_ptr_(other.func_ptr_) {
  }
private:
  Value (*func_ptr_)(Value&, Value&);

};

}  // End expression namespace
}  // End peloton namespace
