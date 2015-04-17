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
  ParserExpression(ExpressionType type, __attribute__((unused)) char* name)
 : AbstractExpression(type), ival(0) {
  }

  ParserExpression(ExpressionType type, __attribute__((unused)) char* name, __attribute__((unused)) char* table)
 : AbstractExpression(type), ival(0) {
  }

  ParserExpression(ExpressionType type)
 : AbstractExpression(type), ival(0) {
  }

  ParserExpression(ExpressionType type, int placeholder)
 : AbstractExpression(type), ival(placeholder) {
  }

  ParserExpression(ExpressionType type, __attribute__((unused)) char* func_name, __attribute__((unused)) AbstractExpression* expr, __attribute__((unused)) bool distinct)
 : AbstractExpression(type), ival(0) {
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

  int ival;

 protected:
  std::string name;
  std::string table;
  std::string alias;

};

} // End expression namespace
} // End nstore namespace

