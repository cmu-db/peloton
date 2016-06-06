/******************************************************************
*
* peloton for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include "parser/node/SQLSets.h"

namespace peloton {
namespace parser {

std::string &SQLSets::toString(std::string &buf)
{
  std::ostringstream oss;
  std::string childNodeStr;
  oss << "SET " << childNodesToString(childNodeStr, ",");
  buf = oss.str();
  return buf;
}

}  // End parser namespace
}  // End peloton namespace
