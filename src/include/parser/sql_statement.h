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

#include <vector>

#include "common/macros.h"
#include "common/printable.h"
#include "common/sql_node_visitor.h"
#include "common/internal_types.h"

namespace peloton {

namespace parser {

struct TableInfo {
  ~TableInfo() {}

  std::string table_name;

  std::string database_name;
};

// Base class for every SQLStatement
class SQLStatement : public Printable {
 public:
  SQLStatement(StatementType type) : stmt_type(type){};

  virtual ~SQLStatement() {}

  virtual StatementType GetType() { return stmt_type; }

  // Get a string representation for debugging
  virtual const std::string GetInfo(int num_indent) const;

  // Get a string representation for debugging
  virtual const std::string GetInfo() const;

  // Visitor Pattern used for the optimizer to access statements
  // This allows a facility outside the object itself to determine the type of
  // class using the built-in type system.
  virtual void Accept(SqlNodeVisitor *v) = 0;

 private:
  StatementType stmt_type;
};

class TableRefStatement : public SQLStatement {
 public:
  TableRefStatement(StatementType type) : SQLStatement(type) {}

  virtual ~TableRefStatement() {}

  virtual inline void TryBindDatabaseName(std::string default_database_name) {
    if (!table_info_) table_info_.reset(new parser::TableInfo());

    if (table_info_->database_name.empty())
      table_info_->database_name = default_database_name;
  }

  virtual inline std::string GetTableName() const {
    return table_info_->table_name;
  }

  // Get the name of the database of this table
  virtual inline std::string GetDatabaseName() const {
    return table_info_->database_name;
  }

  std::unique_ptr<TableInfo> table_info_ = nullptr;
};

// Represents the result of the SQLParser.
// If parsing was successful it is a list of SQLStatement.
class SQLStatementList : public Printable {
 public:
  SQLStatementList()
      : is_valid(true), parser_msg(nullptr), error_line(0), error_col(0){};

  SQLStatementList(SQLStatement *stmt) : is_valid(true), parser_msg(nullptr) {
    AddStatement(stmt);
  };

  virtual ~SQLStatementList() { delete (char *)parser_msg; }

  void AddStatement(SQLStatement *stmt) {
    statements.push_back(std::unique_ptr<SQLStatement>(stmt));
  }

  SQLStatement *GetStatement(int id) const { return statements[id].get(); }

  std::unique_ptr<SQLStatement> PassOutStatement(int id) {
    return std::move(statements[id]);
  }

  void PassInStatement(std::unique_ptr<SQLStatement> stmt) {
    statements.push_back(std::move(stmt));
  }

  const std::vector<std::unique_ptr<SQLStatement>> &GetStatements() const {
    return statements;
  }

  size_t GetNumStatements() const { return statements.size(); }

  // Get a string representation for debugging
  const std::string GetInfo(int num_indent) const;

  // Get a string representation for debugging
  const std::string GetInfo() const;

  std::vector<std::unique_ptr<SQLStatement>> statements;
  bool is_valid;
  const char *parser_msg;
  int error_line;
  int error_col;
};

}  // namespace parser
}  // namespace peloton
