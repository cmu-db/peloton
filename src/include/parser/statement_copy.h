//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_import.h
//
// Identification: src/include/parser/statement_import.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "expression/constant_value_expression.h"
#include "expression/parser_expression.h"

namespace peloton {
namespace parser {

/**
 * @struct CopyStatement
 * @brief Represents PSQL Copy statements.
 */
struct CopyStatement : SQLStatement {

  CopyStatement(CopyType type)
      : SQLStatement(STATEMENT_TYPE_COPY),
        type(type),
        table_name(NULL),
        file_path(NULL),
        delimiter(',') {};

  virtual ~CopyStatement() {
    delete table_name;
    delete file_path;
  }

  // Get the name of the database of this table
  inline std::string GetDatabaseName() {
    if (table_name == nullptr || table_name->database == nullptr) {
      return DEFAULT_DB_NAME;
    }
    return std::string(table_name->database);
  }

  inline std::string GetTableName() { return std::string(table_name->name); }

  CopyType type;

  expression::ParserExpression* table_name;
  char* file_path;
  char delimiter;
};

}  // End parser namespace
}  // End peloton namespace
