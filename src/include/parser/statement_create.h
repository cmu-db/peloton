//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_create.h
//
// Identification: src/include/parser/statement_create.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "expression/parser_expression.h"
#include "common/types.h"

namespace peloton {
namespace parser {

/**
 * @struct ColumnDefinition
 * @brief Represents definition of a table column
 */
struct ColumnDefinition {
  enum DataType {
    INVALID,

    PRIMARY,
    FOREIGN,

    CHAR,
    INT,
    INTEGER,
    TINYINT,
    SMALLINT,
    BIGINT,
    DOUBLE,
    FLOAT,
    DECIMAL,
    BOOLEAN,
    ADDRESS,
    TIMESTAMP,
    TEXT,

    VARCHAR,
    VARBINARY
  };

  ColumnDefinition(DataType type) : type(type) {}

  ColumnDefinition(char* name, DataType type) : name(name), type(type) {}

  virtual ~ColumnDefinition() {
    if (primary_key) {
      for (auto key : *primary_key) free(key);
      delete primary_key;
    }

    if (foreign_key_source) {
      for (auto key : *foreign_key_source) free(key);
      delete foreign_key_source;
    }
    if (foreign_key_sink) {
      for (auto key : *foreign_key_sink) free(key);
      delete foreign_key_sink;
    }

    free(name);
    delete table_name;
  }

  static common::Type::TypeId GetValueType(DataType type) {
    switch (type) {
      case INT:
      case INTEGER:
        return common::Type::INTEGER;
        break;

      case TINYINT:
        return common::Type::TINYINT;
        break;
      case SMALLINT:
        return common::Type::SMALLINT;
        break;
      case BIGINT:
        return common::Type::BIGINT;
        break;

      //case DOUBLE:
      //case FLOAT:
      //  return common::Type::DOUBLE;
      //  break;

      case DECIMAL:
        return common::Type::DECIMAL;
        break;

      case BOOLEAN:
        return common::Type::BOOLEAN;
        break;

      //case ADDRESS:
      //  return common::Type::ADDRESS;
      //  break;

      case TIMESTAMP:
        return common::Type::TIMESTAMP;
        break;

      case CHAR:
      case TEXT:
      case VARCHAR:
        return common::Type::VARCHAR;
        break;

      case VARBINARY:
        return common::Type::VARBINARY;
        break;

      case INVALID:
      case PRIMARY:
      case FOREIGN:
      default:
        return common::Type::INVALID;
        break;
    }
  }

  char* name = nullptr;

  // The name of the table and its database
  expression::ParserExpression* table_name = nullptr;

  DataType type;
  size_t varlen = 0;
  bool not_null = false;
  bool primary = false;
  bool unique = false;
  expression::AbstractExpression* default_value = nullptr;

  std::vector<char*>* primary_key = nullptr;
  std::vector<char*>* foreign_key_source = nullptr;
  std::vector<char*>* foreign_key_sink = nullptr;
};

/**
 * @struct CreateStatement
 * @brief Represents "CREATE TABLE students (name TEXT, student_number INTEGER,
 * city TEXT, grade DOUBLE)"
 */
struct CreateStatement : SQLStatement {
  enum CreateType {
    kTable,
    kDatabase,
    kIndex
  };

  CreateStatement(CreateType type)
      : SQLStatement(STATEMENT_TYPE_CREATE),
        type(type),
        if_not_exists(false),
        columns(NULL) {};

  virtual ~CreateStatement() {
    if (columns) {
      for (auto col : *columns) delete col;
      delete columns;
    }

    if (index_attrs) {
      for (auto attr : *index_attrs) free(attr);
      delete index_attrs;
    }

    free(index_name);
    free(database_name);
    delete table_name;
  }

  CreateType type;
  bool if_not_exists;

  std::vector<ColumnDefinition*>* columns;
  std::vector<char*>* index_attrs = nullptr;

  IndexType index_type;

  inline std::string GetTableName() { return table_name->name; }

  // Get the name of the database of this table
  inline std::string GetDatabaseName() {
    if (table_name == nullptr || table_name->database == nullptr) {
      return DEFAULT_DB_NAME;
    }
    return std::string(table_name->database);
  }

  char* index_name = nullptr;
  char* database_name = nullptr;
  expression::ParserExpression* table_name = nullptr;

  bool unique = false;
};

}  // End parser namespace
}  // End peloton namespace
