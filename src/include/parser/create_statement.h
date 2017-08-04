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

#include <memory>
#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"
#include "type/types.h"

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

  enum FKConstrActionType {
    NOACTION,
    RESTRICT,
    CASCADE,
    SETNULL,
    SETDEFAULT
  };

  enum FKConstrMatchType {
    SIMPLE,
    PARTIAL,
    FULL
  };

  ColumnDefinition(DataType type) : type(type) {
    // Set varlen to TEXT_MAX_LENGTH if the data type is TEXT
    if (type == TEXT)
      varlen = type::PELOTON_TEXT_MAX_LEN;
  }

  ColumnDefinition(char* name, DataType type) : name(name), type(type) {
    // Set varlen to TEXT_MAX_LENGTH if the data type is TEXT
    if (type == TEXT)
      varlen = type::PELOTON_TEXT_MAX_LEN;
  }

  virtual ~ColumnDefinition() {
    if (primary_key) {
      for (auto key : *primary_key) delete[](key);
      delete primary_key;
    }

    if (foreign_key_source) {
      for (auto key : *foreign_key_source) delete[](key);
      delete foreign_key_source;
    }
    if (foreign_key_sink) {
      for (auto key : *foreign_key_sink) delete[](key);
      delete foreign_key_sink;
    }
    delete[] name;
    if (table_info_ != nullptr)
      delete table_info_;
    if (default_value != nullptr)
      delete default_value;
    if (check_expression != nullptr)
      delete check_expression;
  }

  static type::TypeId GetValueType(DataType type) {
    switch (type) {
      case INT:
      case INTEGER:
        return type::TypeId::INTEGER;
        break;

      case TINYINT:
        return type::TypeId::TINYINT;
        break;
      case SMALLINT:
        return type::TypeId::SMALLINT;
        break;
      case BIGINT:
        return type::TypeId::BIGINT;
        break;

      // case DOUBLE:
      // case FLOAT:
      //  return type::Type::DOUBLE;
      //  break;

      case DECIMAL:
      case DOUBLE:
      case FLOAT:
        return type::TypeId::DECIMAL;
        break;

      case BOOLEAN:
        return type::TypeId::BOOLEAN;
        break;

      // case ADDRESS:
      //  return type::Type::ADDRESS;
      //  break;

      case TIMESTAMP:
        return type::TypeId::TIMESTAMP;
        break;

      case CHAR:
      case TEXT:
      case VARCHAR:
        return type::TypeId::VARCHAR;
        break;

      case VARBINARY:
        return type::TypeId::VARBINARY;
        break;

      case INVALID:
      case PRIMARY:
      case FOREIGN:
      default:
        return type::TypeId::INVALID;
        break;
    }
  }

  char* name = nullptr;

  // The name of the table and its database
  TableInfo* table_info_ = nullptr;

  DataType type;
  size_t varlen = 0;
  bool not_null = false;
  bool primary = false;
  bool unique = false;
  expression::AbstractExpression* default_value = nullptr;
  expression::AbstractExpression* check_expression = nullptr;

  std::vector<char*>* primary_key = nullptr;
  std::vector<char*>* foreign_key_source = nullptr;
  std::vector<char*>* foreign_key_sink = nullptr;

  char* foreign_key_table_name = nullptr;
  FKConstrActionType foreign_key_delete_action;
  FKConstrActionType foreign_key_update_action;
  FKConstrMatchType foreign_key_match_type;
};

/**
 * @class CreateStatement
 * @brief Represents "CREATE TABLE students (name TEXT, student_number INTEGER,
 * city TEXT, grade DOUBLE)"
 */
class CreateStatement : public TableRefStatement {
 public:
  enum CreateType { kTable, kDatabase, kIndex, kTrigger };

  CreateStatement(CreateType type)
      : TableRefStatement(StatementType::CREATE),
        type(type),
        if_not_exists(false),
        columns(nullptr){};

  virtual ~CreateStatement() {
    if (columns != nullptr) {
      for (auto col : *columns) delete col;
      delete columns;
    }

    if (index_attrs != nullptr) {
      for (auto attr : *index_attrs) delete[] (attr);
      delete index_attrs;
    }
    if (trigger_funcname) {
      for (auto t : *trigger_funcname) delete[](t);
      delete trigger_funcname;
    }
    if (trigger_args) {
      for (auto t : *trigger_args) delete[](t);
      delete trigger_args;
    }
    if (trigger_columns) {
      for (auto t : *trigger_columns) delete[](t);
      delete trigger_columns;
    }

    if (index_name != nullptr) {
      delete[] (index_name);
    }
    if (trigger_name) {
      delete[](trigger_name);
    }
    if (database_name) {
      delete[](database_name);
    }

    if (trigger_when) {
      delete trigger_when;
    }
  }

  virtual void Accept(SqlNodeVisitor* v) const override { v->Visit(this); }

  CreateType type;
  bool if_not_exists;

  std::vector<ColumnDefinition*>* columns;
  std::vector<char*>* index_attrs = nullptr;

  IndexType index_type;

  char* index_name = nullptr;
  char* trigger_name = nullptr;
  char* database_name = nullptr;

  bool unique = false;

  std::vector<char*>* trigger_funcname = nullptr;
  std::vector<char*>* trigger_args = nullptr;
  std::vector<char*>* trigger_columns = nullptr;
  expression::AbstractExpression* trigger_when = nullptr;
  int16_t trigger_type;  // information about row, timing, events, access by
                         // pg_trigger
};

}  // namespace parser
}  // namespace peloton
