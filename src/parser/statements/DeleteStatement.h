#ifndef __DELETE_STATEMENT_H__
#define __DELETE_STATEMENT_H__

#include "SQLStatement.h"

namespace hsql {


/**
 * @struct DeleteStatement
 * @brief Represents "DELETE FROM students WHERE grade > 3.0"
 *
 * If expr == NULL => delete all rows (truncate)
 */
struct DeleteStatement : SQLStatement {
	DeleteStatement() :
		SQLStatement(kStmtDelete),
		table_name(NULL),
		expr(NULL) {};

	virtual ~DeleteStatement() {
		delete table_name;
		delete expr;
	}


	char* table_name;
	Expr* expr;
};



} // namespace hsql
#endif