/******************************************************************
*
* peloton for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include "parser/node/SQLOrder.h"

namespace peloton {
namespace parser {

const int SQLOrder::UNKOWN = -1;
const int SQLOrder::ASC = 0;
const int SQLOrder::DESC = 1;

static const char *CgSQLOrderStrings[] = {
"ASC",
"DESC",
};

#define CgSQLOrderStringSize (sizeof(CgSQLOrderStrings)/sizeof(CgSQLOrderStrings[0]))

static const char *CgSQLOrderValueToString(int n)
{
  int sql_order_size = CgSQLOrderStringSize;
  if (n < 0 || (sql_order_size - 1) < n)
    return "?";
  return CgSQLOrderStrings[n];
}

static int CgSQLOrderStringToValue(const std::string value)
{
  int sql_order_size = CgSQLOrderStringSize;
  for (int n=0; n<sql_order_size; n++) {
    if (value.compare(CgSQLOrderStrings[n]) == 0)
      return n;
  }
  return SQLOrder::UNKOWN;
}

void SQLOrder::setOrder(int type)
{
  this->order = type;
}

void SQLOrder::setOrder(const std::string &order)
{
  setOrder(CgSQLOrderStringToValue(order));
}

std::string &SQLOrder::toString(std::string &buf)
{
  std::ostringstream oss;
  oss << getValue() << " " << CgSQLOrderValueToString(getOrder());
  buf = oss.str();
  return buf;
}

}  // End parser namespace
}  // End peloton namespace

