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
  FunctionExpression(char * func_name, const std::vector<AbstractExpression*>& children) :
      AbstractExpression(EXPRESSION_TYPE_FUNCTION), func_name_(func_name), func_ptr_(nullptr){
    for (auto &child : children){
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
  }

  FunctionExpression(Value (*func_ptr)(const std::vector<Value>&), Type::TypeId type_id,
      size_t num_args, const std::vector<AbstractExpression*>& children) :
      AbstractExpression(EXPRESSION_TYPE_FUNCTION, type_id), func_ptr_(
          func_ptr), num_args_(num_args) {
    for (auto &child : children){
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
  }

  void SetFunctionExpressionParameters(Value (*func_ptr)(const std::vector<Value>&),
      Type::TypeId val_type, size_t num_args) {
    func_ptr_ = func_ptr;
    value_type_ = val_type;
    if (num_args != children_.size()){
      throw Exception("Unexpected number of arguments to function: "+func_name_+". Expected: "+std::to_string(num_args)+ " Actual: " + std::to_string(children_.size()));
    }

    num_args_ = num_args;
  }

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
  UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
  UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    // for now support only one child
    std::vector<Value> child_values;

    for (auto &child: children_){
      child_values.push_back(child->Evaluate(tuple1, tuple2, context));
    }
    PL_ASSERT(num_args_ == child_values.size());
    return func_ptr_(child_values);
  }

  AbstractExpression * Copy() const{
    return new FunctionExpression(*this);
  }

  std::string func_name_;

protected:
  FunctionExpression(const FunctionExpression& other) :
      AbstractExpression(other), func_name_(other.func_name_), func_ptr_(other.func_ptr_), num_args_(other.num_args_) {
  }
private:
  Value (*func_ptr_)(const std::vector<Value>&);
  size_t num_args_ = 0;


};

}  // End expression namespace
}  // End peloton namespace
