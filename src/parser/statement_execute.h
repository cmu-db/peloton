#pragma once

#include "sql_statement.h"

namespace nstore {
namespace parser {

/**
 * @struct ExecuteStatement
 * @brief Represents "EXECUTE ins_prep(100, "test", 2.3);"
 */
struct ExecuteStatement : SQLStatement {
	ExecuteStatement() :
		SQLStatement(STATEMENT_TYPE_EXECUTE),
		name(NULL),
		parameters(NULL) {}
	
	virtual ~ExecuteStatement() {
	  free(name);
		delete parameters;
	}

	char* name;
	std::vector<Expr*>* parameters;
};


} // End parser namespace
} // End nstore namespace
