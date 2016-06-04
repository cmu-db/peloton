#pragma once

#include "parser/sql_statement.h"

namespace peloton {
namespace parser {

/**
 * @struct ImportStatement
 * @brief Represents SQL Import statements.
 */
struct ImportStatement : SQLStatement {
	enum ImportType {
            kImportCSV,
            kImportTSV,  // Other delimited file formats can be supported in the future
        };

	ImportStatement() :
		SQLStatement(STATEMENT_TYPE_IMPORT),
		type(type),
        file_path(NULL),
        table_name(NULL) {};  // For bulk import support

	virtual ~ImportStatement() {
		free(file_path);
	  	free(table_name);
	}
	
	ImportType type;
	char* file_path;
        char* table_name;

};

} // End parser namespace
} // End peloton namespace
