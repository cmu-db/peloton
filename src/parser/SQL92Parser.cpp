#include <antlr3.h>

#include "parser/SQL92Parser.h"

namespace peloton {
namespace parser {

SQL92Parser::SQL92Parser()
{
}

bool SQL92Parser::parse(const std::string &queryString)
{
  bool parseResult = SQLParser::parse(queryString);
  setStatementType(SQLStatement::SQL92);
  return parseResult;
}

}  // End parser namespace
}  // End peloton namespace
