#pragma once

#include "sql_statement.h"

namespace nstore {
namespace parser {

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
	  free(table_name);
		delete expr;
	}


	char* table_name;
	Expr* expr;
};

} // End parser namespace
} // End nstore namespace
