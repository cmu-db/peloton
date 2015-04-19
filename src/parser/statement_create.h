#pragma once

#include "sql_statement.h"

namespace nstore {
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

  ColumnDefinition(DataType type) :
    type(type) {}


  ColumnDefinition(char* name, DataType type) :
    name(name),
    type(type){}

  virtual ~ColumnDefinition() {
    free(name);
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
 * @brief Represents "CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, grade DOUBLE)"
 */
struct CreateStatement : SQLStatement {
  enum CreateType {
    kTable,
    kDatabase,
    kIndex
  };

  CreateStatement(CreateType type) :
    SQLStatement(STATEMENT_TYPE_CREATE),
    type(type),
    if_not_exists(false),
    columns(NULL),
    name(NULL) {};

  virtual ~CreateStatement() {

    if(columns) {
      for(auto col : *columns)
        delete col;
      delete columns;
    }

    if(index_attrs) {
      for(auto attr : *index_attrs)
        free(attr);
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

} // End parser namespace
} // End nstore namespace
