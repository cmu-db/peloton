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
#include "common/sql_node_visitor.h"
#include "type/value.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// OperatorExpression
//===----------------------------------------------------------------------===//

class FunctionExpression : public AbstractExpression {
 public:
  FunctionExpression(const char* func_name,
                     const std::vector<AbstractExpression*>& children)
      : AbstractExpression(ExpressionType::FUNCTION),
        func_name_(func_name),
        func_ptr_(nullptr) {
    for (auto& child : children) {
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
  }

  FunctionExpression(type::Value (*func_ptr)(const std::vector<type::Value>&),
                     type::TypeId return_type,
                     const std::vector<type::TypeId>& arg_types,
                     const std::vector<AbstractExpression*>& children)
      : AbstractExpression(ExpressionType::FUNCTION, return_type),
        func_ptr_(func_ptr),
        func_arg_types_(arg_types) {
    for (auto& child : children) {
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
    CheckChildrenTypes(children_, func_name_);
  }

  void SetFunctionExpressionParameters(
      type::Value (*func_ptr)(const std::vector<type::Value>&),
      type::TypeId val_type,
      const std::vector<type::TypeId>& arg_types) {
    func_ptr_ = func_ptr;
    return_value_type_ = val_type;
    func_arg_types_ = arg_types;
    CheckChildrenTypes(children_, func_name_);
  }

  type::Value Evaluate(
      const AbstractTuple* tuple1, const AbstractTuple* tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext* context) const override {
    // for now support only one child
    std::vector<type::Value> child_values;
    PL_ASSERT(func_ptr_ != nullptr);
    for (auto& child : children_) {
      child_values.push_back(child->Evaluate(tuple1, tuple2, context));
    }
    type::Value ret = func_ptr_(child_values);
    // if this is false we should throw an exception
    // TODO: maybe checking this every time is not neccesary? but it prevents
    // crashing
    if (ret.GetElementType() != return_value_type_) {
      throw Exception(
          EXCEPTION_TYPE_EXPRESSION,
          "function " + func_name_ + " returned an unexpected type.");
    }
    return ret;
  }

  AbstractExpression* Copy() const override { return new FunctionExpression(*this); }

  std::string func_name_;

  type::Value (*func_ptr_)(const std::vector<type::Value>&) = nullptr;

  std::vector<type::TypeId> func_arg_types_;

  virtual void Accept(SqlNodeVisitor* v) override { v->Visit(this); }

 protected:
  FunctionExpression(const FunctionExpression& other)
      : AbstractExpression(other),
        func_name_(other.func_name_),
        func_ptr_(other.func_ptr_),
        func_arg_types_(other.func_arg_types_) {}

 private:

  // throws an exception if children return unexpected types
  void CheckChildrenTypes(
      const std::vector<std::unique_ptr<AbstractExpression>>& children,
      const std::string& func_name) {
    if (func_arg_types_.size() != children.size()) {
      throw Exception(EXCEPTION_TYPE_EXPRESSION,
                      "Unexpected number of arguments to function: " +
                          func_name + ". Expected: " +
                          std::to_string(func_arg_types_.size()) + " Actual: " +
                          std::to_string(children.size()));
    }
    // check that the types are correct
    for (size_t i = 0; i < func_arg_types_.size(); i++) {
      if (children[i]->GetValueType() != func_arg_types_[i]) {
        throw Exception(EXCEPTION_TYPE_EXPRESSION,
                        "Incorrect argument type to fucntion: " + func_name +
                            ". Argument " + std::to_string(i) +
                            " expected type " +
                            type::Type::GetInstance(func_arg_types_[i])->ToString() +
                            " but found " +
                            type::Type::GetInstance(children[i]->GetValueType())
                                ->ToString() +
                            ".");
      }
    }
  }
};

}  // namespace expression
}  // namespace peloton
