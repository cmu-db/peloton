#pragma once

#include "parser/SQLNode.h"

namespace peloton {
namespace parser {

class SQLCommand : public SQLNode {

public:

  static const int UNKOWN;
  static const int SELECT;
  static const int UPDATE;
  static const int INSERT;
  static const int DEL;
  static const int CREATE;
  static const int DROP;
  static const int CREATE_INDEX;
  static const int DROP_INDEX;
  static const int USE;
  static const int SHOW;

private:

  int commandType;
  bool asyncFlag;
    
public:

  SQLCommand() {
    setType(COMMAND);
    setAsyncEnabled(false);
  }

  const char *getTypeName() {
    return "SQLCommand";
  }
  
  void setCommandType(int commandType);

  int getCommandType() {
    return this->commandType;
  }

  bool isCommandType(int type) {
    return (this->commandType == type) ? true : false;
  }
  
  void setAsyncEnabled(bool asyncFlag) {
    this->asyncFlag = asyncFlag;
  }

  bool isAsync() {
    return this->asyncFlag;
  }

  bool isSync() {
    return !this->asyncFlag;
  }
  
  bool isSelect() {
    return isCommandType(SELECT);
  }

  bool isUpdate() {
    return isCommandType(UPDATE);
  }

  bool isInsert() {
    return isCommandType(INSERT);
  }

  bool isDelete() {
    return isCommandType(DEL);
  }
  
  bool isCreate() {
    return isCommandType(CREATE);
  }

  bool isDrop() {
    return isCommandType(DROP);
  }
  
  bool isCreateIndex() {
    return isCommandType(CREATE_INDEX);
  }
  
  bool isDropIndex() {
    return isCommandType(DROP_INDEX);
  }

  bool isShow() {
    return isCommandType(SHOW);
  }

  bool isUse() {
    return isCommandType(USE);
  }
};

class SQLSelect : public SQLCommand {

public:

  SQLSelect() {
    setCommandType(SELECT);
  }
};

class SQLUpdate : public SQLCommand {

public:

  SQLUpdate() {
    setCommandType(UPDATE);
  }

};

class SQLInsert : public SQLCommand {

public:

  SQLInsert() {
    setCommandType(INSERT);
  }
};

class SQLDelete : public SQLCommand {

public:

  SQLDelete() {
    setCommandType(DEL);
  }
};

class SQLCreate : public SQLCommand {

public:

  SQLCreate() {
    setCommandType(CREATE);
  }
};

class SQLDrop : public SQLCommand {

public:

  SQLDrop() {
    setCommandType(DROP);
  }
};

class SQLCreateIndex : public SQLCommand {

public:

  SQLCreateIndex() {
    setCommandType(CREATE_INDEX);
  }
};

class SQLDropIndex : public SQLCommand {

public:

  SQLDropIndex() {
    setCommandType(DROP_INDEX);
  }
};

class SQLShow : public SQLCommand {

public:

  SQLShow() {
    setCommandType(SHOW);
  }
};

class SQLUse : public SQLCommand {

public:

  SQLUse() {
    setCommandType(USE);
  }
};

}  // End parser namespace
}  // End peloton namespace

