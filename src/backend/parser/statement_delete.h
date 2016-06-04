#pragma once

#include "backend/parser/sql_statement.h"

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
		SQLStatement(STATEMENT_TYPE_DELETE),
		table_name(NULL),
		expr(NULL) {};

	virtual ~DeleteStatement() {
	  free(table_name);
		delete expr;
	}


	char* table_name;
	expression::AbstractExpression* expr;
};

} // End parser namespace
} // End nstore namespace
