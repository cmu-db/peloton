//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_function_statement.h
//
// Identification: src/include/parser/alter_function_statement.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace parser {
/**
 * @struct AlterTableStatement
 * @brief Represents "ALTER TABLE add column COLUMN_NAME COLUMN_TYPE"
 * TODO: add implementation of AlterTableStatement
 */
struct AlterTableStatement : TableRefStatement {
  enum class AlterTableType { INVALID = 0, ADD = 1, DROP = 2, RENAME = 3 };
  AlterTableStatement(AlterTableType type)
      : TableRefStatement(StatementType::ALTER),
        type(type),
        names(new std::vector<char*>),
        oldName(nullptr),
        newName(nullptr)
        {};

  virtual ~AlterTableStatement() {
    /*if (columns != nullptr) {
      for (auto col : *columns) delete col;
      delete columns;
    }*/
    if (names != nullptr) {
      for (auto name : *names) delete name;
      delete names;
    }
    if (oldName) delete oldName;
    if (newName) delete newName;
  }

  virtual void Accept(SqlNodeVisitor* v) override { v->Visit(this); }

  AlterTableType type;

  // Dropped columns
  std::vector<char*>* names;

  // Added columns
  //std::vector<ColumnDefinition*>* columns;

    // the name that needs to be changed
  char *oldName;
  char *newName;
};

}  // End parser namespace
}  // End peloton namespace
