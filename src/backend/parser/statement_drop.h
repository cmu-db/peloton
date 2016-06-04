#pragma once

#include "backend/parser/sql_statement.h"

namespace nstore {
namespace parser {

/**
 * @struct DropStatement
 * @brief Represents "DROP TABLE"
 */
struct DropStatement : SQLStatement {
	enum EntityType {
    kDatabase,
	  kTable,
		kSchema,
		kIndex,
		kView,
		kPreparedStatement
	};


	DropStatement(EntityType type) :
		SQLStatement(STATEMENT_TYPE_DROP),
		type(type),
		name(NULL),
		table_name(NULL){}

	virtual ~DropStatement() {
	  free(name);
	  free(table_name);
	}

	EntityType type;
	char* name;
	char* table_name;
};

} // End parser namespace
} // End nstore namespace
