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
  FunctionExpression(char * func_name, AbstractExpression* child) :
      AbstractExpression(EXPRESSION_TYPE_FUNCTION), func_name_(func_name), func_ptr_(nullptr){
    children_.push_back(std::unique_ptr<AbstractExpression>(child));
  }

  FunctionExpression(Value (*func_ptr)(const Value&), Type::TypeId type_id,
      AbstractExpression *left, AbstractExpression *right) :
      AbstractExpression(EXPRESSION_TYPE_FUNCTION, type_id, left, right), func_ptr_(
          func_ptr) {
  }

  void SetFunctionExpressionParameters(Value (*func_ptr)(const Value&),
      Type::TypeId val_type) {
    func_ptr_ = func_ptr;
    value_type_ = val_type;
  }

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
  UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
  UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    // for now support only one child
    PL_ASSERT(children_.size() >= 1);
    Value v1 = children_[0]->Evaluate(tuple1, tuple2, context);
    return func_ptr_(v1);
  }

  AbstractExpression * Copy() const{
    return new FunctionExpression(*this);
  }

  std::string func_name_;

protected:
  FunctionExpression(const FunctionExpression& other) :
      AbstractExpression(other), func_name_(other.func_name_), func_ptr_(other.func_ptr_) {
  }
private:
  Value (*func_ptr_)(const Value&);


};

}  // End expression namespace
}  // End peloton namespace
