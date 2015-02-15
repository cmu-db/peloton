#ifndef __DROP_STATEMENT_H__
#define __DROP_STATEMENT_H__

#include "SQLStatement.h"

namespace hsql {


/**
 * @struct DropStatement
 * @brief Represents "DROP TABLE"
 */
struct DropStatement : SQLStatement {
	enum EntityType {
		kTable,
		kSchema,
		kIndex,
		kView,
		kPreparedStatement
	};


	DropStatement(EntityType type) :
		SQLStatement(kStmtDrop),
		type(type),
		name(NULL) {}

	virtual ~DropStatement() {
		delete name;
	}


	EntityType type;
	const char* name;
};





} // namespace hsql
#endif