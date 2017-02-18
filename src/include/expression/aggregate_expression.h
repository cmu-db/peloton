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

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// OperatorExpression
//===----------------------------------------------------------------------===//

// This expression should never be evaluated, it is only used for
// parrsing/planning/optimizing
class AggregateExpression : public AbstractExpression {
 public:
  AggregateExpression(ExpressionType type, bool distinct,
                      AbstractExpression* child)
      : AbstractExpression(type) {
    switch (type) {
      distinct_ = distinct;
      case ExpressionType::AGGREGATE_COUNT:
        if (child != nullptr &&
            child->GetExpressionType() == ExpressionType::STAR) {
          delete child;
          child = nullptr;
          exp_type_ = ExpressionType::AGGREGATE_COUNT_STAR;
          expr_name_ = "count(*)";
        } else {
          expr_name_ = "count";
        }
        break;
      case ExpressionType::AGGREGATE_SUM:
        expr_name_ = "sum";
        break;
      case ExpressionType::AGGREGATE_MIN:
        expr_name_ = "min";
        break;
      case ExpressionType::AGGREGATE_MAX:
        expr_name_ = "max";
        break;
      case ExpressionType::AGGREGATE_AVG:
        expr_name_ = "avg";
        break;
      default:
        throw Exception("Aggregate type not supported");
    }
    if (child != nullptr) {
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
  }

  type::Value Evaluate(
      UNUSED_ATTRIBUTE const AbstractTuple* tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple* tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext* context) const override {
    // for now support only one child
    return type::ValueFactory::GetBooleanValue(true);
  }

  AbstractExpression* Copy() const { return new AggregateExpression(*this); }

  void DeduceExpressionType() override {
    switch (exp_type_) {
      // if count return an integer
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR:
        return_value_type_ = type::Type::INTEGER;
        break;
      // return the type of the base
      case ExpressionType::AGGREGATE_MAX:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_SUM:
        PL_ASSERT(children_.size() >= 1);
        return_value_type_ = children_[0]->GetValueType();
        break;
      case ExpressionType::AGGREGATE_AVG:
        return_value_type_ = type::Type::DECIMAL;
        break;
      default:
        break;
    }
  }

  virtual void Accept(SqlNodeVisitor* v) { v->Visit(this); }

 protected:
  AggregateExpression(const AggregateExpression& other)
      : AbstractExpression(other) {}

 private:
};

}  // End expression namespace
}  // End peloton namespace
