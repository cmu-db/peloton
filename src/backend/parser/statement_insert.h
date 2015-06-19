#pragma once

#include "backend/parser/sql_statement.h"

namespace nstore {
namespace parser {

/**
 * @struct InsertStatement
 * @brief Represents "INSERT INTO students VALUES ('Max', 1112233, 'Musterhausen', 2.3)"
 */
struct InsertStatement : SQLStatement {

  InsertStatement(InsertType type) :
    SQLStatement(STATEMENT_TYPE_INSERT),
    type(type),
    table_name(NULL),
    columns(NULL),
    values(NULL),
    select(NULL) {}

  virtual ~InsertStatement() {
    free(table_name);

    if(columns) {
      for(auto col : *columns)
        free(col);
      delete columns;
    }

    if(values){
      for(auto expr : *values){
        if(expr->GetExpressionType() != EXPRESSION_TYPE_PLACEHOLDER)
          delete expr;
      }
      delete values;
    }

    delete select;
  }

  InsertType type;
  char* table_name;
  std::vector<char*>* columns;
  std::vector<expression::AbstractExpression*>* values;
  SelectStatement* select;
};

} // End parser namespace
} // End nstore namespace
