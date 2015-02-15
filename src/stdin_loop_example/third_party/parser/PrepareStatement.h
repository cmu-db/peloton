#ifndef __PREPARE_STATEMENT_H__
#define __PREPARE_STATEMENT_H__

#include "SQLStatement.h"
#include "SelectStatement.h"
#include <algorithm>

namespace hsql {


/**
 * @struct PrepareStatement
 * @brief Represents "PREPARE ins_prep: SELECT * FROM t1 WHERE c1 = ? AND c2 = ?"
 */
struct PrepareStatement : SQLStatement {
	PrepareStatement() :
		SQLStatement(kStmtPrepare),
		name(NULL),
		query(NULL) {}
	
	virtual ~PrepareStatement() {
		delete query;
		delete name;
	}

	/**
	 * @param vector of placeholders that the parser found
	 *
	 * When setting the placeholders we need to make sure that they are in the correct order.
	 * To ensure that, during parsing we store the character position use that to sort the list here.
	 */
	void setPlaceholders(std::vector<void*> ph) {
		for (void* e : ph) {
			if (e != NULL)
				placeholders.push_back((Expr*) e);
		}
		// Sort by col-id
		std::sort(placeholders.begin(), placeholders.end(), [](Expr* i, Expr* j) -> bool { return (i->ival < j->ival); });

		// Set the placeholder id on the Expr. This replaces the previously stored column id
		for (uint i = 0; i < placeholders.size(); ++i) placeholders[i]->ival = i;
	}

	const char* name;
	SQLStatementList* query;
	std::vector<Expr*> placeholders;
};




} // namsepace hsql
#endif