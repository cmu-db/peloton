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
#include "parser/create_statement.h"

namespace peloton {
namespace parser {
/**
 * @struct AlterTableStatement
 * @brief Represents "ALTER TABLE add column COLUMN_NAME COLUMN_TYPE"
 * TODO: add implementation of AlterTableStatement
 */
class AlterTableStatement : public TableRefStatement {
 public:
  enum class AlterTableType { INVALID = 0, ALTER = 1, RENAME = 2 };
  AlterTableStatement(AlterTableType type)
      : TableRefStatement(StatementType::ALTER),
        type(type),
        oldName(nullptr),
        newName(nullptr) {
    dropped_names =
        type == AlterTableType::RENAME ? nullptr : (new std::vector<char *>);
    added_columns = type == AlterTableType::RENAME
                        ? nullptr
                        : (new std::vector<std::unique_ptr<ColumnDefinition>>);
    changed_type_columns =
        type == AlterTableType::RENAME
            ? nullptr
            : (new std::vector<std::unique_ptr<ColumnDefinition>>);
  }

  virtual ~AlterTableStatement() {
    if (added_columns != nullptr) {
      delete added_columns;
    }
    if (dropped_names != nullptr) {
      for (auto name : *dropped_names) delete name;
      delete dropped_names;
    }
    if (changed_type_columns != nullptr) {
      delete changed_type_columns;
    }
    if (oldName) delete oldName;
    if (newName) delete newName;
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  AlterTableType type;

  // Dropped columns
  std::vector<char *> *dropped_names;

  // Added columns
  std::vector<std::unique_ptr<ColumnDefinition>> *added_columns;

  std::vector<std::unique_ptr<ColumnDefinition>> *changed_type_columns;
  // the name that needs to be changed
  char *oldName;
  char *newName;
};

}  // End parser namespace
}  // End peloton namespace
