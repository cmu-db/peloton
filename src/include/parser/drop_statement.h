//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_drop.h
//
// Identification: src/include/parser/statement_drop.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace parser {

/**
 * @struct DropStatement
 * @brief Represents "DROP TABLE"
 */
struct DropStatement : TableRefStatement {
  enum EntityType {
    kDatabase,
    kTable,
    kSchema,
    kIndex,
    kView,
    kPreparedStatement
  };

  DropStatement(EntityType type)
      : TableRefStatement(StatementType::DROP), type(type), missing(false) {}

  virtual ~DropStatement() {}

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  EntityType type;
  std::unique_ptr<char[]> database_name = nullptr;
  std::unique_ptr<char[]> index_name = nullptr;
  std::unique_ptr<char[]> prep_stmt = nullptr;
  bool missing;
};

}  // End parser namespace
}  // End peloton namespace
