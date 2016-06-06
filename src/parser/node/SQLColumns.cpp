
#include "parser/SQLStatement.h"
#include "parser/node/SQLColumns.h"

namespace peloton {
namespace parser {

std::string &SQLColumns::toString(std::string &buf)
{
  return toExpressionString(buf);
}

}  // End parser namespace
}  // End peloton namespace
