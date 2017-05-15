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

#include <string>
#include "expression/abstract_expression.h"
#include "common/sql_node_visitor.h"
#include "catalog/function_catalog.h"
#include "udf/udf_main.h"
#include "common/logger.h"

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
                     type::Type::TypeId return_type,
                     const std::vector<type::Type::TypeId>& arg_types,
                     const std::vector<AbstractExpression*>& children)
      : AbstractExpression(ExpressionType::FUNCTION, return_type),
        func_ptr_(func_ptr){
    for (auto& child : children) {
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
    CheckChildrenTypes(arg_types, children_, func_name_);
  }

  // For a built-in function
  void SetFunctionExpressionParameters(
      type::Value (*func_ptr)(const std::vector<type::Value>&),
      type::Type::TypeId val_type,
      const std::vector<type::Type::TypeId>& arg_types) {
    func_ptr_ = func_ptr;
    return_value_type_ = val_type;
    CheckChildrenTypes(arg_types, children_, func_name_);
  }

  void SetUDFType(bool is_udf) {
    is_udf_ = is_udf;
  }

  bool GetUDFType() {
    return is_udf_;
  }

  type::Value Evaluate(
      const AbstractTuple* tuple1, const AbstractTuple* tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext* context) const override {
    // for now support only one child
    std::vector<type::Value> child_values;
    for (auto& child : children_) {
      child_values.push_back(child->Evaluate(tuple1, tuple2, context));
    }
    type::Value ret;

    if(is_udf_) {

      LOG_DEBUG("The function is highly likely a UDF");

      // Populate the necessary fields
      auto func_catalog = catalog::FunctionCatalog::GetInstance();
      catalog::UDFFunctionData func_data =
        func_catalog->GetFunction(func_name_, context->GetTransaction());

      if(func_data.func_is_present_) {
        CheckChildrenTypes(func_data.argument_types_, children_, func_name_);
        // TODO: check if the func_body is aligned with the grammer
        // TODO: save it back to catalogc
        // This is to remove "; "
        //auto func_body = func_data.func_string_.substr(0, func_data.func_string_.size()-2);
        auto func_body = func_data.func_string_;
        size_t num_index_0 = func_body.find("i + 1", 0);
        size_t num_index_1 = func_body.find("a + b", 0);
        if (num_index_0 != std::string::npos)
          func_body.replace(num_index_0, 5, "i+1");
        if (num_index_1 != std::string::npos) {
          func_body.replace(num_index_1, 5, "a+b");
        }
        auto udf_handle = new peloton::udf::UDFHandle(func_body,
            func_data.argument_names_, func_data.argument_types_, func_data.return_type_);

        // Try to compile it
        ret = udf_handle->Execute(child_values);
        delete udf_handle;

        if (ret.GetElementType() != func_data.return_type_) {
          throw Exception(
              EXCEPTION_TYPE_EXPRESSION,
              "function " + func_name_ + " returned an unexpected type.");
        }
      }
      else {
         throw Exception("function " + func_name_ + " not found. in pg_proc");
      }
    }
    else {
      PL_ASSERT(func_ptr_ != nullptr);
      ret = func_ptr_(child_values);

      // if this is false we should throw an exception
      // TODO: maybe checking this every time is not neccesary? but it prevents
      // crashing
      if (ret.GetElementType() != return_value_type_) {
        throw Exception(
            EXCEPTION_TYPE_EXPRESSION,
            "function " + func_name_ + " returned an unexpected type.");
      }
    }
    
    return ret;
  }

  void DeduceExpressionType() override {
    if(is_udf_)
      return_value_type_ = type::Type::INTEGER;
  }

  AbstractExpression* Copy() const { return new FunctionExpression(*this); }

  std::string func_name_;

  virtual void Accept(SqlNodeVisitor* v) { v->Visit(this); }

 protected:
  FunctionExpression(const FunctionExpression& other)
      : AbstractExpression(other),
        func_name_(other.func_name_),
        func_ptr_(other.func_ptr_),
        is_udf_(other.is_udf_) {}

 private:
  type::Value (*func_ptr_)(const std::vector<type::Value>&) = nullptr;

  // For UDFs
  bool is_udf_ = false;

  // throws an exception if children return unexpected types
  static void CheckChildrenTypes(
      const std::vector<type::Type::TypeId>& arg_types,
      const std::vector<std::unique_ptr<AbstractExpression>>& children,
      const std::string& func_name) {
    if (arg_types.size() != children.size()) {
      throw Exception(EXCEPTION_TYPE_EXPRESSION,
                      "Unexpected number of arguments to function: " +
                          func_name + ". Expected: " +
                          std::to_string(arg_types.size()) + " Actual: " +
                          std::to_string(children.size()));
    }
    // check that the types are correct
    for (size_t i = 0; i < arg_types.size(); i++) {
      if (children[i]->GetValueType() != arg_types[i]) {
        throw Exception(EXCEPTION_TYPE_EXPRESSION,
                        "Incorrect argument type to fucntion: " + func_name +
                            ". Argument " + std::to_string(i) +
                            " expected type " +
                            type::Type::GetInstance(arg_types[i])->ToString() +
                            " but found " +
                            type::Type::GetInstance(children[i]->GetValueType())
                                ->ToString() +
                            ".");
      }
    }
  }
};

}  // End expression namespace
}  // End peloton namespace
