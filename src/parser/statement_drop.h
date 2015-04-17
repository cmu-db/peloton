#pragma once

#include "sql_statement.h"

namespace nstore {
namespace parser {

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
	  free(name);
	}


	EntityType type;
	char* name;
};

} // End parser namespace
} // End nstore namespace
