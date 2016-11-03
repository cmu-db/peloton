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
        delimiter(NULL) {};

  virtual ~CopyStatement() {
    free(file_path);
    delete table_name;
  }

  // Get the name of the database of this table
  inline std::string GetDatabaseName() {
    if (table_name == nullptr || table_name->database == nullptr) {
      return DEFAULT_DB_NAME;
    }
    return std::string(table_name->database);
  }

  inline std::string GetTableName() { return std::string(table_name->name); }

  inline char GetDelimiter() {
    auto val_expression =
        static_cast<expression::ConstantValueExpression*>(delimiter);
    auto value = val_expression->GetValue();
    if (value.GetLength() > 1) {
      throw ParserException("Single character delimiter expected");
    }
    std::cout << "The delimiter string is: " + value.ToString() << std::endl;

    return *(value.GetData());
  }

  inline std::string GetFilePath() {
    auto val_expression =
        static_cast<expression::ConstantValueExpression*>(file_path);
    return val_expression->GetValue().ToString();
  }

  CopyType type;
  expression::ParserExpression* table_name;
  expression::AbstractExpression* file_path;
  expression::AbstractExpression* delimiter;
};

}  // End parser namespace
}  // End peloton namespace
