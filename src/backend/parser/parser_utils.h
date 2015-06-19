#pragma once

#include "backend/parser/statements.h"
#include "backend/expression/expression.h"

namespace nstore {
namespace parser {

void GetSelectStatementInfo(SelectStatement* stmt, uint num_indent);
void GetInsertStatementInfo(InsertStatement* stmt, uint num_indent);
void GetCreateStatementInfo(CreateStatement* stmt, uint num_indent);
void GetExpressionInfo(const expression::AbstractExpression* expr, uint num_indent);

} // End parser namespace
} // End nstore namespace
