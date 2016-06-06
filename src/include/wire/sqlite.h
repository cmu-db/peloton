//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sqlite.h
//
// Identification: src/include/wire/sqlite.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdlib.h>
#include <stdio.h>
#include <mutex>

#include "wire/portal.h"

struct sqlite3;

namespace peloton {
namespace wire {

#define WIRE_INTEGER 1
#define WIRE_TEXT 2
#define WIRE_FLOAT 3
#define WIRE_NULL 4

// used to synch sqlite accesses
extern std::mutex sqlite_mutex;

class Sqlite {
 public:
  Sqlite();

  virtual ~Sqlite();

  // PortalExec - Execute query string
  virtual int PortalExec(const char *query, std::vector<ResType> &res,
                         std::vector<FieldInfoType> &info, int &rows_change,
                         std::string &err_msg);

  // InitBindPrepStmt - Prepare and bind a query from a query string
  int PrepareStmt(const char *query, sqlite3_stmt **stmt, std::string &err_msg);

  int BindStmt(std::vector<std::pair<int, std::string>> &parameters,
               sqlite3_stmt **stmt, std::string &err_msg);

  // GetRowDesc - Get RowDescription of a query
  void GetRowDesc(void *stmt, std::vector<FieldInfoType> &info);

  // ExecPrepStmt - Execute a statement from a prepared and bound statement
  int ExecPrepStmt(void *stmt, bool unnamed, std::vector<ResType> &res,
                   int &rows_change, std::string &err_msg);

  static void CopyFromTo(const char *src, std::vector<unsigned char> &dst);

  static int ExecCallback(void *res, int argc, char **argv, char **azColName);

  static int GetSize(const std::string &);

  void Test();

 private:
  // SQLite database
  struct sqlite3 *sqlite_db_;
};

}  // End wire namespace
}  // End peloton namespace
