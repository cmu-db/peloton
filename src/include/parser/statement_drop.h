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
#include "expression/parser_expression.h"

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
      : TableRefStatement(STATEMENT_TYPE_DROP), type(type), missing(false) {}

  virtual ~DropStatement() {
    free(database_name);
    free(index_name);
    free(prep_stmt);
  }

  EntityType type;
  char* database_name = nullptr;
  char* index_name = nullptr;
  char* prep_stmt = nullptr;
  bool missing;
};

}  // End parser namespace
}  // End peloton namespace
