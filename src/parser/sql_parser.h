#pragma once

#include "statements.h"
#include "parser_bison.h"
#include "parser_flex.h"

namespace nstore {
namespace parser {

/*!
 * \mainpage SQLParser (C++)
 */

/*!
 * @brief Main class for parsing SQL strings
 */
class SQLParser {
public:
	static SQLStatementList* parseSQLString(const char* sql);
	static SQLStatementList* parseSQLString(const std::string& sql);

private:
	SQLParser();
};

} // End parser namespace
} // End nstore namespace
