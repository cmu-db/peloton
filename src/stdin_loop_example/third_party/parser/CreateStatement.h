#ifndef __CREATE_STATEMENT_H__
#define __CREATE_STATEMENT_H__

#include "SQLStatement.h"

namespace hsql {

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
		delete name;
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
		SQLStatement(kStmtCreate),
		type(type),
		if_not_exists(false),
		columns(NULL),
		file_path(NULL),
		table_name(NULL) {};

	virtual ~CreateStatement() {
		delete columns;
		delete file_path;
		delete table_name;
	}

	CreateType type;
	bool if_not_exists;

	std::vector<ColumnDefinition*>* columns;

	const char* file_path;
	const char* table_name;
};



} // namespace hsql
#endif