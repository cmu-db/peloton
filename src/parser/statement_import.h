#pragma once

#include "sql_statement.h"

namespace nstore {
namespace parser {

/**
 * @struct ImportStatement
 * @brief Represents "IMPORT"
 */
struct ImportStatement : SQLStatement {
	enum ImportType {
		kImportCSV,
		kImportTbl, // Hyrise file format
	};

	ImportStatement(ImportType type) :
		SQLStatement(STATEMENT_TYPE_IMPORT),
		type(type),
		name(NULL),
		file_path(NULL),
		table_name(NULL) {};
		
	virtual ~ImportStatement() {
	  free(file_path);
	  free(table_name);
	}

	ImportType type;
	char* name;
	char* file_path;
	char* table_name;
};

} // End parser namespace
} // End nstore namespace
