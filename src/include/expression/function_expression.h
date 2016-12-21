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

class FunctionExpression: public AbstractExpression {
public:
  FunctionExpression(const char * func_name, const std::vector<AbstractExpression*>& children) :
      AbstractExpression(EXPRESSION_TYPE_FUNCTION), func_name_(func_name), func_ptr_(nullptr){
    for (auto &child : children){
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
  }

  FunctionExpression(type::Value (*func_ptr)(const std::vector<type::Value>&), type::Type::TypeId return_type,
      size_t num_args, const std::vector<AbstractExpression*>& children) :
      AbstractExpression(EXPRESSION_TYPE_FUNCTION, return_type), func_ptr_(
          func_ptr), num_args_(num_args) {
    if (num_args != children_.size()){
      throw Exception("Unexpected number of arguments to function: "+func_name_+". Expected: "+std::to_string(num_args)+ " Actual: " + std::to_string(children_.size()));
    }
    for (auto &child : children){
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
  }

  void SetFunctionExpressionParameters(type::Value (*func_ptr)(const std::vector<type::Value>&),
                                       type::Type::TypeId val_type, size_t num_args) {
    func_ptr_ = func_ptr;
    return_value_type_ = val_type;
    if (num_args != children_.size()){
      throw Exception("Unexpected number of arguments to function: "+func_name_+". Expected: "+std::to_string(num_args)+ " Actual: " + std::to_string(children_.size()));
    }

    num_args_ = num_args;
  }

  type::Value Evaluate(const AbstractTuple *tuple1,
  const AbstractTuple *tuple2,
  UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    // for now support only one child
    std::vector<type::Value> child_values;
    PL_ASSERT(func_ptr_ != nullptr);
    for (auto &child: children_){
      child_values.push_back(child->Evaluate(tuple1, tuple2, context));
    }
    // this should always be true because we check
    PL_ASSERT(num_args_ == child_values.size());
    type::Value ret = func_ptr_(child_values);
    // if this is false we should throw an exception
    if (ret.GetElementType() != return_value_type_){
      throw Exception("function "+func_name_+" returned an unexpected type.");
    }
    return ret;
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
  type::Value (*func_ptr_)(const std::vector<type::Value>&) = nullptr;
  size_t num_args_ = 0;


};

}  // End expression namespace
}  // End peloton namespace
