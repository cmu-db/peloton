//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_utils.cpp
//
// Identification: src/parser/parser_utils.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/parser_utils.h"
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "expression/tuple_value_expression.h"

#include <stdio.h>
#include <string>
#include <iostream>
#include <sstream>

namespace peloton {
namespace parser {

std::string ParserUtils::GetOperatorExpression(
    const expression::AbstractExpression *expr, uint num_indent) {
  if (expr == NULL) {
    return StringUtil::Indent(num_indent) + "null";
  }

  std::ostringstream os;
  os << GetExpressionInfo(expr->GetChild(0), num_indent);
  if (expr->GetChild(1) != NULL)
    os << "\n" << GetExpressionInfo(expr->GetChild(1), num_indent);
  return os.str();
}

std::string ParserUtils::GetExpressionInfo(
    const expression::AbstractExpression *expr, uint num_indent) {
  if (expr == NULL) {
    return StringUtil::Indent(num_indent) + "null";
  }

  std::ostringstream os;
  os << StringUtil::Indent(num_indent)
     << "-> Expr Type :: " << ExpressionTypeToString(expr->GetExpressionType())
     << "\n";

  switch (expr->GetExpressionType()) {
    case ExpressionType::STAR:
      os << StringUtil::Indent(num_indent + 1) << "*\n";
      break;
    case ExpressionType::VALUE_TUPLE:
      os << StringUtil::Indent(num_indent + 1) << expr->GetInfo() << "\n";
      os << StringUtil::Indent(num_indent + 1)
         << ((expression::TupleValueExpression *)expr)->GetTableName() << "<";
      os << ((expression::TupleValueExpression *)expr)->GetColumnName()
         << ">\n";
      break;
    case ExpressionType::COMPARE_GREATERTHAN:
      os << StringUtil::Indent(num_indent + 1) << expr->GetInfo() << "\n";
      for (size_t i = 0; i < (expr)->GetChildrenSize(); ++i) {
        os << StringUtil::Indent(num_indent + 1)
           << ((expr)->GetChild(i))->GetInfo() << "\n";
      }
      break;
    case ExpressionType::VALUE_CONSTANT:
      os << StringUtil::Indent(num_indent + 1) << expr->GetInfo() << "\n";
      break;
    case ExpressionType::FUNCTION_REF:
      os << StringUtil::Indent(num_indent + 1) << expr->GetInfo() << "\n";
      break;
    default:
      os << GetOperatorExpression(expr, num_indent + 1);
      break;
  }

  // TODO: Fix this
  if (expr->alias.size() != 0) {
    os << StringUtil::Indent(num_indent) << "Alias: " << expr->alias;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

}  // namespace parser
}  // namespace peloton
