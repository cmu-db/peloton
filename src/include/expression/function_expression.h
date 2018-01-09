//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_expression.h
//
// Identification: src/include/expression/function_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "function/functions.h"
#include "type/value.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// Expressions that call built-in functions.
//===----------------------------------------------------------------------===//

class FunctionExpression : public AbstractExpression {
 public:
  FunctionExpression(const char *func_name,
                     const std::vector<AbstractExpression *> &children)
      : AbstractExpression(ExpressionType::FUNCTION),
        func_name_(func_name),
        func_(OperatorId::Invalid, nullptr) {
    for (auto &child : children) {
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
  }

  FunctionExpression(function::BuiltInFuncType func_ptr,
                     type::TypeId return_type,
                     const std::vector<type::TypeId> &arg_types,
                     const std::vector<AbstractExpression *> &children)
      : AbstractExpression(ExpressionType::FUNCTION, return_type),
        func_(func_ptr),
        func_arg_types_(arg_types) {
    for (auto &child : children) {
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
    CheckChildrenTypes(children_, func_name_);
  }

  void SetFunctionExpressionParameters(
      function::BuiltInFuncType func_ptr, type::TypeId val_type,
      const std::vector<type::TypeId> &arg_types) {
    func_ = func_ptr;
    return_value_type_ = val_type;
    func_arg_types_ = arg_types;
    CheckChildrenTypes(children_, func_name_);
  }

  type::Value Evaluate(
      const AbstractTuple *tuple1, const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    std::vector<type::Value> child_values;
    PL_ASSERT(func_.impl != nullptr);
    for (auto &child : children_) {
      child_values.push_back(child->Evaluate(tuple1, tuple2, context));
    }

    type::Value ret = func_.impl(child_values);

    // TODO: Checking this every time is not necessary, but it prevents crashing
    if (ret.GetElementType() != return_value_type_) {
      throw Exception(
          ExceptionType::EXPRESSION,
          "function " + func_name_ + " returned an unexpected type.");
    }

    return ret;
  }

  AbstractExpression *Copy() const override {
    return new FunctionExpression(*this);
  }

  const std::string &GetFuncName() const { return func_name_; }

  const function::BuiltInFuncType &GetFunc() const { return func_; }

  const std::vector<type::TypeId> &GetArgTypes() const {
    return func_arg_types_;
  }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

 private:
  std::string func_name_;

  function::BuiltInFuncType func_;

  std::vector<type::TypeId> func_arg_types_;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 protected:
  FunctionExpression(const FunctionExpression &other)
      : AbstractExpression(other),
        func_name_(other.func_name_),
        func_(other.func_),
        func_arg_types_(other.func_arg_types_) {}

 private:
  // throws an exception if children return unexpected types
  void CheckChildrenTypes(
      const std::vector<std::unique_ptr<AbstractExpression>> &children,
      const std::string &func_name) {
    if (func_arg_types_.size() != children.size()) {
      throw Exception(ExceptionType::EXPRESSION,
                      "Unexpected number of arguments to function: " +
                          func_name + ". Expected: " +
                          std::to_string(func_arg_types_.size()) + " Actual: " +
                          std::to_string(children.size()));
    }
    // check that the types are correct
    for (size_t i = 0; i < func_arg_types_.size(); i++) {
      if (children[i]->GetValueType() != func_arg_types_[i]) {
        throw Exception(ExceptionType::EXPRESSION,
                        "Incorrect argument type to function: " + func_name +
                            ". Argument " + std::to_string(i) +
                            " expected type " +
                            TypeIdToString(func_arg_types_[i]) + " but found " +
                            TypeIdToString(children[i]->GetValueType()) + ".");
      }
    }
  }
};

}  // namespace expression
}  // namespace peloton
