//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_expression_parse.h
//
// Identification: src/include/parser/peloton/abstract_expression_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/abstract_parse.h"

#include "type/types.h"

namespace peloton {
namespace parser {

class AbstractExpressionParse : public AbstractParse {
 public:
  AbstractExpressionParse() = delete;
  AbstractExpressionParse(const AbstractExpressionParse &) = delete;
  AbstractExpressionParse &operator=(const AbstractExpressionParse &) = delete;
  AbstractExpressionParse(AbstractExpressionParse &&) = delete;
  AbstractExpressionParse &operator=(AbstractExpressionParse &&) = delete;

  virtual ExpressionType GetExpressionType() const = 0;
};

}  // namespace parser
}  // namespace peloton
