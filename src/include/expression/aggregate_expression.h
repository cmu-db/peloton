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
// This expression should never be evaluated, it is only used for parrsing/planning/optimizing
class AggregateExpression: public AbstractExpression {
public:
  AggregateExpression(ExpressionType type, bool distinct, AbstractExpression* child) :
      AbstractExpression(type){
    switch(type){
    distinct_ = distinct;
    case EXPRESSION_TYPE_AGGREGATE_COUNT:
      if (child != nullptr && child->GetExpressionType() == EXPRESSION_TYPE_STAR){
        delete child;
        child = nullptr;
        exp_type_ = EXPRESSION_TYPE_AGGREGATE_COUNT_STAR;
        expr_name_ = "count(*)";
      }else{
        expr_name_ = "count";
      }
      break;
    case EXPRESSION_TYPE_AGGREGATE_SUM:
      expr_name_ = "sum";
      break;
    case EXPRESSION_TYPE_AGGREGATE_MIN:
      expr_name_ = "min";
      break;
    case EXPRESSION_TYPE_AGGREGATE_MAX:
      expr_name_ = "max";
      break;
    case EXPRESSION_TYPE_AGGREGATE_AVG:
      expr_name_ = "avg";
      break;
    default:
      throw Exception("Aggregate type not supported");
    }
    if (child != nullptr){
      children_.push_back(std::unique_ptr<AbstractExpression>(child));
    }
  }


  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
  UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
  UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    // for now support only one child
    return common::ValueFactory::GetBooleanValue(true);
  }

  AbstractExpression * Copy() const{
    return new AggregateExpression(*this);
  }

protected:
  AggregateExpression(const AggregateExpression& other) :
      AbstractExpression(other){
  }
private:

};

}  // End expression namespace
}  // End peloton namespace
