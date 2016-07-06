//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_delete.h
//
// Identification: src/include/parser/statement_delete.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"

namespace peloton {
namespace parser {

/**
 * @struct DeleteStatement
 * @brief Represents "DELETE FROM students WHERE grade > 3.0"
 *
 * If expr == NULL => delete all rows (truncate)
 */
struct DeleteStatement : SQLStatement {
  DeleteStatement()
      : SQLStatement(STATEMENT_TYPE_DELETE), table_name(NULL), expr(NULL){};

  virtual ~DeleteStatement() {
    free(table_name);
    delete expr;
  }

  char* table_name;
  expression::AbstractExpression* expr;
};

}  // End parser namespace
}  // End peloton namespace
