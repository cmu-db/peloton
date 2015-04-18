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

#include "expression/abstract_expression.h"

#include <string>

namespace nstore {
namespace expression {

//===--------------------------------------------------------------------===//
// Parser Expression
//===--------------------------------------------------------------------===//

class ParserExpression : public AbstractExpression {
 public:
  ParserExpression(ExpressionType type, char* name)
 : AbstractExpression(type) {
    m_name = std::string(name);
  }

  ParserExpression(ExpressionType type, char* name, char* column)
 : AbstractExpression(type) {
    m_name = std::string(name);
    m_column = std::string(column);
  }

  ParserExpression(ExpressionType type)
 : AbstractExpression(type) {
  }

  ParserExpression(ExpressionType type, char* func_name, AbstractExpression* expr, bool distinct)
 : AbstractExpression(type) {
    m_name = std::string(func_name);
    m_expr = expr;
    m_distinct = distinct;
  }

  ParserExpression(ExpressionType type, int placeholder)
 : AbstractExpression(type) {
    ival = placeholder;
  }

  virtual ~ParserExpression() {
  }

  Value Evaluate(__attribute__((unused)) const storage::Tuple *tuple1, __attribute__((unused)) const storage::Tuple *tuple2) const {
    return Value::GetTrue();
  }

  std::string DebugInfo(__attribute__((unused)) const std::string &spacer) const {
    std::stringstream os;
    return os.str();
  }

};

} // End expression namespace
} // End nstore namespace

