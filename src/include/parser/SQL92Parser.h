/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQL92PARSER_H_
#define _USQL_SQL92PARSER_H_

#include "parser/SQLParser.h"

namespace uSQL {

class SQL92Parser : public SQLParser {

public:

  SQL92Parser();

  bool parse(const std::string &queryString);
};

}

#endif
