
#ifndef __SQLHELPER_H__
#define __SQLHELPER_H__


#include "sqllib.h"

namespace hsql {

void printSelectStatementInfo(SelectStatement* stmt, uint num_indent);
void printImportStatementInfo(ImportStatement* stmt, uint num_indent);
void printInsertStatementInfo(InsertStatement* stmt, uint num_indent);
void printCreateStatementInfo(CreateStatement* stmt, uint num_indent);
void printExpression(Expr* expr, uint num_indent);

} // namespace hsql

#endif