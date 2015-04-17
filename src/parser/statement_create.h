#pragma once

#include "sql_statement.h"

namespace nstore {
namespace parser {

/**
 * @struct ColumnDefinition
 * @brief Represents definition of a table column
 */
struct ColumnDefinition {
	enum DataType {
		TEXT,
		INT,
		DOUBLE
	};

	ColumnDefinition(char* name, DataType type) :
		name(name),
		type(type) {}

	virtual ~ColumnDefinition() {
	  free(name);
	}

	char* name;
	DataType type;
};


/**
 * @struct CreateStatement
 * @brief Represents "CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, grade DOUBLE)"
 */
struct CreateStatement : SQLStatement {
	enum CreateType {
		kTable,
		kTableFromTbl, // Hyrise file format
	};

	CreateStatement(CreateType type) :
		SQLStatement(STATEMENT_TYPE_CREATE),
		type(type),
		if_not_exists(false),
		columns(NULL),
		file_path(NULL),
		table_name(NULL) {};

	virtual ~CreateStatement() {
		delete columns;
		free(file_path);
		free(table_name);
	}

	CreateType type;
	bool if_not_exists;

	std::vector<ColumnDefinition*>* columns;

	char* file_path;
	char* table_name;
};

} // End parser namespace
} // End nstore namespace
