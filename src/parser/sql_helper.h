#pragma once

#include "statements.h"

namespace nstore {
namespace parser {

void printSelectStatementInfo(SelectStatement* stmt, uint num_indent);
void printImportStatementInfo(ImportStatement* stmt, uint num_indent);
void printInsertStatementInfo(InsertStatement* stmt, uint num_indent);
void printCreateStatementInfo(CreateStatement* stmt, uint num_indent);
void printExpression(Expr* expr, uint num_indent);

} // End parser namespace
} // End nstore namespace
