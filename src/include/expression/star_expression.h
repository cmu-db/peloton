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

#include "expression/star_expression.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// OperatorExpression
//===----------------------------------------------------------------------===//

using namespace peloton::common;

class StarExpression: public AbstractExpression {
public:
  StarExpression() :
      AbstractExpression(EXPRESSION_TYPE_STAR){
  }

  StarExpression(Value (*func_ptr)(Value&), Type::TypeId type_id,
      AbstractExpression *left, AbstractExpression *right) :
      AbstractExpression(EXPRESSION_TYPE_STAR, type_id, left, right) {
  }


  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
  UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
  UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    return common::ValueFactory::GetBooleanValue(true);
  }

  AbstractExpression * Copy() const{
    return new StarExpression(*this);
  }

protected:
  StarExpression(const FunctionExpression& other) :
      AbstractExpression(other) {
  }
private:

};

}  // End expression namespace
}  // End peloton namespace
