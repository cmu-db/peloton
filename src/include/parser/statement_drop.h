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
      : SQLStatement(STATEMENT_TYPE_DROP), type(type), missing(false) {}

  virtual ~DropStatement() {
    free(database_name);
    free(index_name);
    free(prep_stmt);
    delete table_name;
  }

  inline std::string GetTableName() { return std::string(table_name->name); }

  inline std::string GetDatabaseName() {
    if (table_name == nullptr || table_name->database == nullptr) {
      return DEFAULT_DB_NAME;
    }
    return std::string(table_name->database);
  }

  EntityType type;
  char* database_name = nullptr;
  char* index_name = nullptr;
  char* prep_stmt = nullptr;
  expression::ParserExpression* table_name = nullptr;
  bool missing;
};

}  // End parser namespace
}  // End peloton namespace
