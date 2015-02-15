#ifndef __EXECUTE_STATEMENT_H__
#define __EXECUTE_STATEMENT_H__

#include "SQLStatement.h"

namespace hsql {


/**
 * @struct ExecuteStatement
 * @brief Represents "EXECUTE ins_prep(100, "test", 2.3);"
 */
struct ExecuteStatement : SQLStatement {
	ExecuteStatement() :
		SQLStatement(kStmtExecute),
		name(NULL),
		parameters(NULL) {}
	
	virtual ~ExecuteStatement() {
		delete name;
		delete parameters;
	}

	const char* name;
	std::vector<Expr*>* parameters;
};




} // namsepace hsql
#endif