#pragma once

#include "sql_statement.h"

namespace nstore {
namespace parser {

/**
 * @struct InsertStatement
 * @brief Represents "INSERT INTO students VALUES ('Max', 1112233, 'Musterhausen', 2.3)"
 */
struct InsertStatement : SQLStatement {
	enum InsertType {
		kInsertValues,
		kInsertSelect
	};

	InsertStatement(InsertType type) :
		SQLStatement(STATEMENT_TYPE_INSERT),
		type(type),
		table_name(NULL),
		columns(NULL),
		values(NULL),
		select(NULL) {}
	
	virtual ~InsertStatement() {
		free(table_name);
		delete columns;
		delete values;
		delete select;
	}

	InsertType type;
	char* table_name;
	std::vector<char*>* columns;
	std::vector<expression::AbstractExpression*>* values;
	SelectStatement* select;
};

} // End parser namespace
} // End nstore namespace
