/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLERROR_H_
#define _USQL_SQLERROR_H_

#include <string>

namespace uSQL {

class SQLError {

private:
  
  unsigned int  code;
  unsigned int  line;
  unsigned int  offset;
  std::string   message;

public:
  
  SQLError();
  ~SQLError();

  void setCode(int value) {
    this->code = value;
  }
  
  int getCode() {
    return this->code;
  }
  
  void setLine(int value) {
    this->line = value;
  }
  
  int getLine() {
    return this->line;
  }
  
  void setOffset(int value) {
    this->offset = value;
  }
  
  int getOffset() {
    return this->offset;
  }
  
  void setMessage(const std::string &message) {
    this->message = message;
  }
  
  const std::string &getMessage() {
    return this->message;
  }

  void clear();
};

}

#endif
