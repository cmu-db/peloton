#pragma once

#include "parser/sql_statement.h"

namespace peloton {
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

	  if(parameters){
	    for(auto expr : *parameters)
	      delete expr;
	  }

		delete parameters;
	}

	char* name;
	std::vector<expression::AbstractExpression*>* parameters;
};


} // End parser namespace
} // End peloton namespace
