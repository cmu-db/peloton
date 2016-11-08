//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_insert.h
//
// Identification: src/include/parser/statement_insert.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "parser/statement_select.h"
#include "expression/parser_expression.h"

namespace peloton {
namespace parser {

/**
 * @struct InsertStatement
 * @brief Represents "INSERT INTO students VALUES ('Max', 1112233,
 * 'Musterhausen', 2.3)"
 */
struct InsertStatement : SQLStatement {
  InsertStatement(InsertType type)
      : SQLStatement(STATEMENT_TYPE_INSERT),
        type(type),
        table_name(NULL),
        columns(NULL),
        insert_values(NULL),
        select(NULL) {}

  virtual ~InsertStatement() {
    delete table_name;

    if (columns) {
      for (auto col : *columns) free(col);
      delete columns;
    }

    if (insert_values) {
      for (auto tuple : *insert_values) {
        for( auto expr : *tuple){
          if (expr->GetExpressionType() != EXPRESSION_TYPE_PLACEHOLDER)
            delete expr;
        }
        delete tuple;
      }
      delete insert_values;
    }

    delete select;
  }

  // Get the name of the database of this table
  inline std::string GetDatabaseName() {
    if (table_name == nullptr || table_name->database == nullptr) {
      return DEFAULT_DB_NAME;
    }
    return std::string(table_name->database);
  }

  inline std::string GetTableName() { return std::string(table_name->name); }

  InsertType type;
  expression::ParserExpression* table_name;
  std::vector<char*>* columns;
  std::vector<std::vector<peloton::expression::AbstractExpression*>*>* insert_values;
  SelectStatement* select;
};

}  // End parser namespace
}  // End peloton namespace
