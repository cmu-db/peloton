//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcop.h
//
// Identification: src/include/tcop/tcop.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdlib.h>
#include <stdio.h>
#include <mutex>
#include <vector>

#include "common/portal.h"
#include "common/statement.h"

namespace peloton {
namespace tcop {

//===--------------------------------------------------------------------===//
// TRAFFIC COP
//===--------------------------------------------------------------------===//

class TrafficCop {

  TrafficCop(TrafficCop const &) = delete;

 public:

  // global singleton
  static TrafficCop &GetInstance(void);

  TrafficCop();
  ~TrafficCop();

  // PortalExec - Execute query string
  int PortalExec(const char *query, std::vector<ResType> &res,
                 std::vector<FieldInfoType> &info, int &rows_change,
                 std::string &err_msg);

  // InitBindPrepStmt - Prepare and bind a query from a query string
  int PrepareStmt(const char *query, PreparedStatement **stmt, std::string &err_msg);

  int BindStmt(std::vector<std::pair<int, std::string>> &parameters,
               PreparedStatement **stmt, std::string &err_msg);

  // GetRowDesc - Get RowDescription of a query
  void GetRowDesc(void *stmt, std::vector<FieldInfoType> &info);

  // ExecPrepStmt - Execute a statement from a prepared and bound statement
  int ExecPrepStmt(void *stmt, bool unnamed, std::vector<ResType> &res,
                   int &rows_change, std::string &err_msg);

  static void CopyFromTo(const char *src, std::vector<unsigned char> &dst);

  static int ExecCallback(void *res, int argc, char **argv, char **azColName);

};

}  // End tcop namespace
}  // End peloton namespace
