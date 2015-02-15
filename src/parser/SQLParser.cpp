#include "SQLParser.h"
#include "bison_parser.h"
#include "flex_lexer.h"
#include <stdio.h>


namespace hsql {

SQLParser::SQLParser() {
	fprintf(stderr, "SQLParser only has static methods atm! Do not initialize!\n");
}


SQLStatementList* SQLParser::parseSQLString(const char *text) {
    SQLStatementList* result;
    yyscan_t scanner;
    YY_BUFFER_STATE state;

    if (hsql_lex_init(&scanner)) {
        // couldn't initialize
        fprintf(stderr, "[Error] SQLParser: Error when initializing lexer!\n");
        return NULL;
    }

    state = hsql__scan_string(text, scanner);

    if (hsql_parse(&result, scanner)) {
        // Returns an error stmt object
        return result;
    }

    hsql__delete_buffer(state, scanner);

    hsql_lex_destroy(scanner);
    return result;
}

SQLStatementList* SQLParser::parseSQLString(const std::string& text) {
    return parseSQLString(text.c_str());
}

    
} // namespace hsql