//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_utils.h
//
// Identification: src/include/parser/parser_utils.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statements.h"
#include "expression/abstract_expression.h"
#include "common/internal_types.h"

namespace peloton {
namespace parser {

class ParserUtils {
 public:
  static std::string GetExpressionInfo(
      const expression::AbstractExpression *expr, uint num_indent);
  static std::string GetOperatorExpression(
      const expression::AbstractExpression *expr, uint num_indent);
};

}  // namespace parser
}  // namespace peloton
