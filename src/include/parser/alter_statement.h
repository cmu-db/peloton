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
class AlterTableStatement : public TableRefStatement {
 public:
  enum class AlterTableType { INVALID = 0, ADD = 1, DROP = 2 };
  AlterTableStatement(AlterTableType type)
      : TableRefStatement(StatementType::ALTER),
        type(type),
        names(new std::vector<char *>) {}

  virtual ~AlterTableStatement() {
    if (names != nullptr) {
      for (auto name : *names) delete name;
      delete names;
    }
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(UNUSED_ATTRIBUTE int num_indent) const override {
    return std::string{};
  }

  const std::string GetInfo() const override { return std::string{}; }

  AlterTableType type;

  // Dropped columns
  std::vector<char *> *names;

  // Added columns
  // std::vector<ColumnDefinition*>* columns;
};

}  // End parser namespace
}  // End peloton namespace
