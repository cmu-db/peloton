//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_statement.h
//
// Identification: src/include/parser/alter_statement.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/create_statement.h"

namespace peloton {
namespace parser {

/**
 * @struct AlterTableStatement
 * @brief Represents "ALTER TABLE add column COLUMN_NAME COLUMN_TYPE"
 */
class AlterTableStatement : public TableRefStatement {
 public:
  enum class AlterTableType { INVALID = 0, ALTER = 1, RENAME = 2 };

  explicit AlterTableStatement(AlterTableType type)
      : TableRefStatement(StatementType::ALTER),
        type(type) {}

  virtual ~AlterTableStatement() {

  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  AlterTableType type;

  // Dropped columns
  std::vector<std::string> dropped_names;

  // Added columns
  std::vector<std::unique_ptr<ColumnDefinition>> added_columns;

  std::vector<std::unique_ptr<ColumnDefinition>> changed_type_columns;

  // the name that needs to be changed
  std::string oldName;
  std::string newName;
};

}  // End parser namespace
}  // End peloton namespace