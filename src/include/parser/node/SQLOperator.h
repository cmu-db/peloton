#pragma once

#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

class SQLOperator : public SQLExpression {

public:

  static const int UNKNOWN;
  static const int SEQ;
  static const int DEQ;
  static const int LT;
  static const int LE;
  static const int GT;
  static const int GE;
  static const int NOTEQ;
  static const int AND;
  static const int OR;

private:
  
  int value;

public:

  SQLOperator() {
    setType(OPERATOR);
  }
  
  const char *getTypeName() {
    return "SQLOperator";
  }
  
  void setValue(int value) {
    this->value = value;
  }

  int getValue() {
    return this->value;
  }

  bool isSEQ() {
    return (this->value == SEQ) ? true : false;
  }
  
  bool isDEQ() {
    return (this->value == DEQ) ? true : false;
  }
  
  bool isLT() {
    return (this->value == LT) ? true : false;
  }

  bool isLE() {
    return (this->value == LE) ? true : false;
  }
  
  bool isGT() {
    return (this->value == GT) ? true : false;
  }

  bool isGE() {
    return (this->value == GE) ? true : false;
  }
  
  bool isNotEQ() {
    return (this->value == NOTEQ) ? true : false;
  }
  
  bool isAnd() {
    return (this->value == AND) ? true : false;
  }
  
  bool isOr() {
    return (this->value == OR) ? true : false;
  }
  
  SQLExpression *getLeftExpression() {
    return getExpression(0);
  }
    
  SQLExpression *getRightExpression() {
    return getExpression(1);
  }

  std::string &toString(std::string &buf);
};

class SQLOperatorSEQ : public SQLOperator {

public:

  SQLOperatorSEQ() {
    setValue(SEQ);
  }

};

class SQLOperatorDEQ : public SQLOperator {

public:

  SQLOperatorDEQ() {
    setValue(DEQ);
  }

};

class SQLOperatorLT : public SQLOperator {

public:

  SQLOperatorLT() {
    setValue(LT);
  }

};

class SQLOperatorLE : public SQLOperator {

public:

  SQLOperatorLE() {
    setValue(LE);
  }

};

class SQLOperatorGT : public SQLOperator {

public:

  SQLOperatorGT() {
    setValue(GT);
  }

};

class SQLOperatorGE : public SQLOperator {

public:

  SQLOperatorGE() {
    setValue(GE);
  }

};

class SQLOperatorNotEQ : public SQLOperator {

public:

  SQLOperatorNotEQ() {
    setValue(NOTEQ);
  }

};

class SQLOperatorAnd : public SQLOperator {

public:

  SQLOperatorAnd() {
    setValue(AND);
  }

};

class SQLOperatorOr : public SQLOperator {

public:

  SQLOperatorOr() {
    setValue(OR);
  }

};

}  // End parser namespace
}  // End peloton namespace
