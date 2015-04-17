#pragma once

#include "sql_statement.h"

namespace nstore {
namespace parser {

/**
 * @struct UpdateClause
 * @brief Represents "column = value" expressions
 */
struct UpdateClause {
	char* column;
	Expr* value;
};


/**
 * @struct UpdateStatement
 * @brief Represents "UPDATE"
 */
struct UpdateStatement : SQLStatement {
	UpdateStatement() :
		SQLStatement(kStmtUpdate),
		table(NULL),
		updates(NULL),
		where(NULL) {}
	
	virtual ~UpdateStatement() {
		delete table;
		delete updates;
		delete where;
	}

	// TODO: switch to char* instead of TableRef
	TableRef* table;
	std::vector<UpdateClause*>* updates;
	Expr* where;
};

} // End parser namespace
} // End nstore namespace
