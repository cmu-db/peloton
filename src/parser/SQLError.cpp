/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include "parser/SQLError.h"

uSQL::SQLError::SQLError()
{
  clear();
}

uSQL::SQLError::~SQLError()
{
}

void uSQL::SQLError::clear()
{
  setCode(-1);
  setLine(-1);
  setOffset(-1);
  setMessage("");
}
