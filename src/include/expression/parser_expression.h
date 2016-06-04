/*-------------------------------------------------------------------------
 *
 * parser_expression.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/expression/parser_expression.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "common/types.h"
#include "expression/abstract_expression.h"

#include <string>

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Parser Expression
//===--------------------------------------------------------------------===//

class ParserExpression : public AbstractExpression {
 public:
  ParserExpression(ExpressionType type, char* name_)
 : AbstractExpression(type, VALUE_TYPE_VARCHAR) {
    name = name_;
  }

  ParserExpression(ExpressionType type, char* name_, char* column_)
 : AbstractExpression(type, VALUE_TYPE_VARCHAR) {
    name = name_;
    column = column_;
  }

  ParserExpression(ExpressionType type)
 : AbstractExpression(type, VALUE_TYPE_VARCHAR) {
  }

  ParserExpression(ExpressionType type, char* func_name_, AbstractExpression* expr_, bool distinct_)
 : AbstractExpression(type, VALUE_TYPE_VARCHAR) {
    name = func_name_;
    expr = expr_;
    distinct = distinct_;
  }

  ParserExpression(ExpressionType type, int placeholder)
 : AbstractExpression(type, VALUE_TYPE_VARCHAR) {
    ival = placeholder;
  }

  virtual ~ParserExpression() {
  }

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
                 UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
                 UNUSED_ATTRIBUTE executor::ExecutorContext *context) const {
    return Value::GetTrue();
  }

  std::string DebugInfo(__attribute__((unused)) const std::string &spacer) const {
    std::stringstream os;
    return os.str();
  }

};

} // End expression namespace
} // End peloton namespace
