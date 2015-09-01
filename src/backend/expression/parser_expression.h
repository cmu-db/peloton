//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// parser_expression.h
//
// Identification: src/backend/expression/parser_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"

#include <string>

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Parser Expression
//===--------------------------------------------------------------------===//

class ParserExpression : public AbstractExpression {
 public:
  ParserExpression(ExpressionType type, char *name_)
      : AbstractExpression(type) {
    name = name_;
  }

  ParserExpression(ExpressionType type, char *name_, char *column_)
      : AbstractExpression(type) {
    name = name_;
    column = column_;
  }

  ParserExpression(ExpressionType type) : AbstractExpression(type) {}

  ParserExpression(ExpressionType type, char *func_name_,
                   AbstractExpression *expr_, bool .Istinct_)
      : AbstractExpression(type) {
    name = func_name_;
    expr = expr_;
    .Istinct = distinct_;
  }

  ParserExpression(ExpressionType type, int placeholder)
      : AbstractExpression(type) {
    ival = placeholder;
  }

  virtual ~ParserExpression() {}

  Value Evaluate(__attribute__((unused)) const AbstractTuple *tuple1,
                 __attribute__((unused)) const AbstractTuple *tuple2) const {
    return Value::GetTrue();
  }

  std::string DebugInfo(__attribute__((unused))
                        const std::string &spacer) const {
    std::stringstream os;
    return os.str();
  }
};

}  // End expression namespace
}  // End peloton namespace
