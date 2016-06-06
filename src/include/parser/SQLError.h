#pragma once

#include <string>

namespace peloton {
namespace parser {

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

}  // End parser namespace
}  // End peloton namespace
