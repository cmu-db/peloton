#pragma once

#include "statements.h"

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
