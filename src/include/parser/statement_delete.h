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

#include "common/logger.h"

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
      : SQLStatement(STATEMENT_TYPE_DELETE), table_name(NULL), expr(NULL) {};

  virtual ~DeleteStatement() {
    delete table_name;
    delete expr;
  }

  inline std::string GetTableName() { return std::string(table_name->name); }

  inline std::string GetDatabaseName() {
    if (table_name == nullptr || table_name->database == nullptr) {
      return DEFAULT_DB_NAME;
    }
    return std::string(table_name->database);
  }

  expression::ParserExpression* table_name = nullptr;

  expression::AbstractExpression* expr = nullptr;
};

}  // End parser namespace
}  // End peloton namespace
