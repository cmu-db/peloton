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
  }

  static ValueType GetValueType(DataType type) {
    switch (type) {
      case INT:
      case INTEGER:
        return VALUE_TYPE_INTEGER;
        break;

      case TINYINT:
        return VALUE_TYPE_TINYINT;
        break;
      case SMALLINT:
        return VALUE_TYPE_SMALLINT;
        break;
      case BIGINT:
        return VALUE_TYPE_BIGINT;
        break;

      case DOUBLE:
      case FLOAT:
        return VALUE_TYPE_DOUBLE;
        break;

      case DECIMAL:
        return VALUE_TYPE_DECIMAL;
        break;

      case BOOLEAN:
        return VALUE_TYPE_BOOLEAN;
        break;

      case ADDRESS:
        return VALUE_TYPE_ADDRESS;
        break;

      case TIMESTAMP:
        return VALUE_TYPE_TIMESTAMP;
        break;

      case CHAR:
      case TEXT:
      case VARCHAR:
        return VALUE_TYPE_VARCHAR;
        break;

      case VARBINARY:
        return VALUE_TYPE_VARBINARY;
        break;

      case INVALID:
      case PRIMARY:
      case FOREIGN:
      default:
        return VALUE_TYPE_INVALID;
        break;
    }
  }

  char* name = nullptr;
  DataType type;
  size_t varlen = 0;
  bool not_null = false;
  bool primary = false;
  bool unique = false;

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
  enum CreateType { kTable, kDatabase, kIndex };

  CreateStatement(CreateType type)
      : SQLStatement(STATEMENT_TYPE_CREATE),
        type(type),
        if_not_exists(false),
        columns(NULL),
        name(NULL){};

  virtual ~CreateStatement() {
    if (columns) {
      for (auto col : *columns) delete col;
      delete columns;
    }

    if (index_attrs) {
      for (auto attr : *index_attrs) free(attr);
      delete index_attrs;
    }

    free(name);
    free(table_name);
  }

  CreateType type;
  bool if_not_exists;

  std::vector<ColumnDefinition*>* columns;
  std::vector<char*>* index_attrs = nullptr;

  char* name;
  char* table_name = nullptr;
  bool unique = false;
};

}  // End parser namespace
}  // End peloton namespace
