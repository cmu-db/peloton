//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_expression.h
//
// Identification: src/include/expression/function_expression.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
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
                     const std::vector<AbstractExpression *> &children);

  FunctionExpression(function::BuiltInFuncType func_ptr,
                     type::TypeId return_type,
                     const std::vector<type::TypeId> &arg_types,
                     const std::vector<AbstractExpression *> &children);

  type::Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                       executor::ExecutorContext *context) const override;

  void SetFunctionExpressionParameters(
      function::BuiltInFuncType func_ptr, type::TypeId val_type,
      const std::vector<type::TypeId> &arg_types);

  AbstractExpression *Copy() const override {
    return new FunctionExpression(*this);
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  const std::string &GetFuncName() const { return func_name_; }

  const function::BuiltInFuncType &GetFunc() const { return func_; }

  const std::vector<type::TypeId> &GetArgTypes() const {
    return func_arg_types_;
  }

 protected:
  FunctionExpression(const FunctionExpression &other)
      : AbstractExpression(other),
        func_name_(other.func_name_),
        func_(other.func_),
        func_arg_types_(other.func_arg_types_) {}

 private:
  // throws an exception if children return unexpected types
  void CheckChildrenTypes() const;

 private:
  // The name of the function
  std::string func_name_;

  // The function implementation
  function::BuiltInFuncType func_;

  // The argument types
  std::vector<type::TypeId> func_arg_types_;
};

}  // namespace expression
}  // namespace peloton
