/******************************************************************
*
* peloton for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include "parser/node/SQLCommand.h"

namespace peloton {
namespace parser {

const int SQLCommand::UNKOWN = -1;
const int SQLCommand::SELECT = 0;
const int SQLCommand::UPDATE = 1;
const int SQLCommand::INSERT = 2;
const int SQLCommand::DEL= 3;
const int SQLCommand::CREATE = 4;
const int SQLCommand::DROP = 5;
const int SQLCommand::CREATE_INDEX = 6;
const int SQLCommand::DROP_INDEX = 7;
const int SQLCommand::SHOW = 8;
const int SQLCommand::USE = 9;

void SQLCommand::setCommandType(int commandType)
{
  this->commandType = commandType;
  
  std::string commandValue = "";
  
  switch (this->commandType) {
  case SELECT:
    commandValue = "SELECT";
    break;
  case UPDATE:
    commandValue = "UPDATE";
    break;
  case INSERT:
    commandValue = "INSERT INTO";
    break;
  case DEL:
    commandValue = "DELETE FROM";
    break;
  case CREATE:
    commandValue = "CREATE COLLECTION";
    break;
  case DROP:
    commandValue = "DROP COLLECTION";
    break;
  case CREATE_INDEX:
    commandValue = "CREATE INDEX";
    break;
  case DROP_INDEX:
    commandValue = "DROP INDEX";
    break;
  case SHOW:
    commandValue = "SHOW";
    break;
  case USE:
    commandValue = "USE";
    break;
  }
  
  setValue(commandValue);
}

}  // End parser namespace
}  // End peloton namespace

