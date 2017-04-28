//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_statement.h
//
// Identification: src/include/parser/sql_statement.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/*
 * SQLStatement.h
 * Definition of the structure used to build the syntax tree.
 */

#pragma once

#include <iostream>
#include <vector>

#include "common/sql_node_visitor.h"
#include "common/macros.h"
#include "common/printable.h"
#include "type/types.h"

namespace peloton {

namespace parser {

struct TableInfo {
  ~TableInfo() {
    if (table_name != nullptr)
      delete[] table_name;
    if (database_name != nullptr)
      delete[] database_name;
  }
  char* table_name = nullptr;
  ;
  char* database_name = nullptr;
};

// Base class for every SQLStatement
class SQLStatement : public Printable {
 public:
  SQLStatement(StatementType type) : stmt_type(type){};

  virtual ~SQLStatement() {}

  virtual StatementType GetType() { return stmt_type; }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Visitor Pattern used for the optimizer to access statements
  // This allows a facility outside the object itself to determine the type of
  // class using the built-in type system.
  virtual void Accept(SqlNodeVisitor* v) const = 0;

 private:
  StatementType stmt_type;
};

class TableRefStatement : public SQLStatement {
 public:
  TableRefStatement(StatementType type) : SQLStatement(type) {}

  virtual ~TableRefStatement() {
    if (table_info_ != nullptr) {
      delete table_info_;
    }
  }

  virtual inline std::string GetTableName() const { return table_info_->table_name; }

  // Get the name of the database of this table
  virtual inline std::string GetDatabaseName() const {
    if (table_info_->database_name == nullptr) {
      return DEFAULT_DB_NAME;
    }
    return table_info_->database_name;
  }

  TableInfo* table_info_ = nullptr;
};

// Represents the result of the SQLParser.
// If parsing was successful it is a list of SQLStatement.
class SQLStatementList : public Printable {
 public:
  SQLStatementList()
      : is_valid(true), parser_msg(NULL), error_line(0), error_col(0){};

  SQLStatementList(SQLStatement* stmt) : is_valid(true), parser_msg(NULL) {
    AddStatement(stmt);
  };

  virtual ~SQLStatementList() {
    // clean up statements
    for (auto stmt : statements) delete stmt;

    free((char*)parser_msg);
  }

  void AddStatement(SQLStatement* stmt) { statements.push_back(stmt); }

  SQLStatement* GetStatement(int id) const { return statements[id]; }

  const std::vector<SQLStatement*>& GetStatements() const { return statements; }

  size_t GetNumStatements() const { return statements.size(); }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  std::vector<SQLStatement*> statements;
  bool is_valid;
  const char* parser_msg;
  int error_line;
  int error_col;
};

}  // End parser namespace
}  // End peloton namespace
