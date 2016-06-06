#pragma once

#include "parser/SQLParser.h"

namespace peloton {
namespace parser {

class SQL92Parser : public SQLParser {

public:

  SQL92Parser();

  bool parse(const std::string &queryString);
};

}  // End parser namespace
}  // End peloton namespace
