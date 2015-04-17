
#include "sql_parser.h"
#include "parser_bison.h"
#include "parser_flex.h"

namespace nstore {
namespace parser {

SQLParser::SQLParser() {
  fprintf(stderr, "SQLParser only has static methods atm! Do not initialize!\n");
}

SQLStatementList* SQLParser::parseSQLString(const char *text) {
  SQLStatementList* result;
  yyscan_t scanner;
  YY_BUFFER_STATE state;

  if (yylex_init(&scanner)) {
    // couldn't initialize
    fprintf(stderr, "[Error] SQLParser: Error when initializing lexer!\n");
    return NULL;
  }

  state = yy_scan_string(text, scanner);

  if (parser_parse(&result, scanner)) {
    // Returns an error stmt object
    return result;
  }

  yy_delete_buffer(state, scanner);

  yylex_destroy(scanner);
  return result;
}

SQLStatementList* SQLParser::parseSQLString(const std::string& text) {
  return parseSQLString(text.c_str());
}

} // End parser namespace
} // End nstore namespace
