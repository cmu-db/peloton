
#include "parser/SQLError.h"

namespace peloton {
namespace parser {

SQLError::SQLError()
{
  clear();
}

SQLError::~SQLError()
{
}

void SQLError::clear()
{
  setCode(-1);
  setLine(-1);
  setOffset(-1);
  setMessage("");
}


}  // End parser namespace
}  // End peloton namespace
