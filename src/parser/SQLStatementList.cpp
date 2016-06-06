#include <sstream>

#include "parser/SQLStatement.h"

namespace peloton {
namespace parser {

SQLStatementList::SQLStatementList()
{
}

SQLStatementList::~SQLStatementList()
{
  clear();
}

void SQLStatementList::clear()
{
  /* FIXME
  SQLStatementList::iterator stmt = begin();
  while (stmt != end()) {
    stmt = erase(stmt);
    delete (*stmt);
  }
  */
  std::vector<SQLStatement *>::clear();
}

}  // End parser namespace
}  // End peloton namespace
