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
#include "type/types.h"

namespace peloton {
namespace parser {

<<<<<<< 81add772e8e4a9d7c11547d9e025a95b2df65fe4
class ParserUtils {
=======
class ParserUtils
{
>>>>>>> Fix for Issue #736
public:
  static std::string GetSelectStatementInfo(SelectStatement* stmt,
                                            uint num_indent);
  static std::string GetInsertStatementInfo(InsertStatement* stmt,
                                            uint num_indent);
  static std::string GetCreateStatementInfo(CreateStatement* stmt,
                                            uint num_indent);
  static std::string GetDeleteStatementInfo(DeleteStatement* stmt,
                                            uint num_indent);
  static std::string GetUpdateStatementInfo(UpdateStatement* stmt,
                                            uint num_indent);
  static std::string GetCopyStatementInfo(CopyStatement* stmt,
                                          uint num_indent);
  static std::string GetExpressionInfo(
                       const expression::AbstractExpression* expr,
                       uint num_indent);
  static std::string GetOperatorExpression(
                       const expression::AbstractExpression* expr,
                       uint num_indent);
  static std::string GetTableRefInfo(const TableRef* table, uint num_indent);
<<<<<<< 81add772e8e4a9d7c11547d9e025a95b2df65fe4
private:
  static std::string indent(uint num_indent);
=======
>>>>>>> Fix for Issue #736
};

}  // namespace parser
}  // namespace peloton
