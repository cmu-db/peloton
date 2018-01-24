//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_expression.cpp
//
// Identification: src/expression/function_expression.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/function_expression.h"

namespace peloton {
namespace expression {

expression::FunctionExpression::FunctionExpression(
    const char *func_name, const std::vector<AbstractExpression *> &children)
    : AbstractExpression(ExpressionType::FUNCTION),
      func_name_(func_name),
      func_(OperatorId::Invalid, nullptr),
      is_udf_(false) {
  for (auto &child : children) {
    children_.push_back(std::unique_ptr<AbstractExpression>(child));
  }
}

expression::FunctionExpression::FunctionExpression(
    function::BuiltInFuncType func_ptr, type::TypeId return_type,
    const std::vector<type::TypeId> &arg_types,
    const std::vector<AbstractExpression *> &children)
    : AbstractExpression(ExpressionType::FUNCTION, return_type),
      func_(func_ptr),
      func_arg_types_(arg_types),
      is_udf_(false) {
  for (auto &child : children) {
    children_.push_back(std::unique_ptr<AbstractExpression>(child));
  }
  CheckChildrenTypes();
}

type::Value FunctionExpression::Evaluate(
    const AbstractTuple *tuple1, const AbstractTuple *tuple2,
    UNUSED_ATTRIBUTE executor::ExecutorContext *context) const {
  std::vector<type::Value> child_values;

  PL_ASSERT(func_.impl != nullptr);
  for (auto &child : children_) {
    child_values.push_back(child->Evaluate(tuple1, tuple2, context));
  }

  type::Value ret = func_.impl(child_values);

  // TODO: Checking this every time is not necessary, but it prevents crashing
  if (ret.GetElementType() != return_value_type_) {
    throw Exception(ExceptionType::EXPRESSION,
                    "function " + func_name_ + " returned an unexpected type.");
  }

  return ret;
}

void FunctionExpression::SetBuiltinFunctionExpressionParameters(
    function::BuiltInFuncType func_ptr, type::TypeId val_type,
    const std::vector<type::TypeId> &arg_types) {
  is_udf_ = false;
  func_ = func_ptr;
  return_value_type_ = val_type;
  func_arg_types_ = arg_types;
  CheckChildrenTypes();
}

void FunctionExpression::SetUDFFunctionExpressionParameters(
    std::shared_ptr<peloton::codegen::CodeContext> func_context,
    type::TypeId val_type, const std::vector<type::TypeId> &arg_types) {
  is_udf_ = true;
  func_context_ = func_context;
  return_value_type_ = val_type;
  func_arg_types_ = arg_types;
  CheckChildrenTypes();
}

const std::string FunctionExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1) << "expression type = Function,\n"
     << StringUtil::Indent(num_indent + 1) << "function name: " << func_name_
     << "\n"
     << StringUtil::Indent(num_indent + 1) << "function args: " << std::endl;

  for (const auto &child : children_) {
    os << child->GetInfo(num_indent + 2);
  }

  return os.str();
}

const std::string FunctionExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

void FunctionExpression::CheckChildrenTypes() const {
  if (func_arg_types_.size() != children_.size()) {
    throw Exception(
        ExceptionType::EXPRESSION,
        "Unexpected number of arguments to function: " + func_name_ +
            ". Expected: " + std::to_string(func_arg_types_.size()) +
            " Actual: " + std::to_string(children_.size()));
  }
  // check that the types are correct
  for (size_t i = 0; i < func_arg_types_.size(); i++) {
    if (children_[i]->GetValueType() != func_arg_types_[i]) {
      throw Exception(ExceptionType::EXPRESSION,
                      "Incorrect argument type to function: " + func_name_ +
                          ". Argument " + std::to_string(i) +
                          " expected type " +
                          TypeIdToString(func_arg_types_[i]) + " but found " +
                          TypeIdToString(children_[i]->GetValueType()) + ".");
    }
  }
}

}  // namespace expression
}  // namespace peloton
