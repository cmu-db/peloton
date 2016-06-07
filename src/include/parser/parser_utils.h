//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_utils.h
//
// Identification: src/include/parser/parser_utils.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statements.h"
#include "expression/abstract_expression.h"
#include "common/types.h"

namespace peloton {
namespace parser {

void GetSelectStatementInfo(SelectStatement* stmt, uint num_indent);
void GetInsertStatementInfo(InsertStatement* stmt, uint num_indent);
void GetCreateStatementInfo(CreateStatement* stmt, uint num_indent);
void GetExpressionInfo(const expression::AbstractExpression* expr,
                       uint num_indent);

}  // End parser namespace
}  // End peloton namespace
