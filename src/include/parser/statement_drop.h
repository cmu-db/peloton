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

namespace peloton {
namespace parser {

/**
 * @struct DropStatement
 * @brief Represents "DROP TABLE"
 */
struct DropStatement : SQLStatement {
  enum EntityType {
    kDatabase,
    kTable,
    kSchema,
    kIndex,
    kView,
    kPreparedStatement
  };

  DropStatement(EntityType type)
      : SQLStatement(STATEMENT_TYPE_DROP),
        type(type),
        name(NULL),
        table_name(NULL) {}

  virtual ~DropStatement() {
    free(name);
    free(table_name);
  }

  EntityType type;
  char* name;
  char* table_name;
};

}  // End parser namespace
}  // End peloton namespace
