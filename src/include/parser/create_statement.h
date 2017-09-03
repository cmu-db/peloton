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
  enum class DataType {
    INVALID,

    PRIMARY,
    FOREIGN,
    MULTIUNIQUE,

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

  ColumnDefinition(DataType type) : type(type) {
    // Set varlen to TEXT_MAX_LENGTH if the data type is TEXT
    if (type == DataType::TEXT)
      varlen = type::PELOTON_TEXT_MAX_LEN;
  }

  ColumnDefinition(char* name, DataType type) : name(name), type(type) {
    // Set varlen to TEXT_MAX_LENGTH if the data type is TEXT
    if (type == DataType::TEXT)
      varlen = type::PELOTON_TEXT_MAX_LEN;
  }

  virtual ~ColumnDefinition() {}

  static type::TypeId GetValueType(DataType type) {
    switch (type) {
      case DataType::INT:
      case DataType::INTEGER:
        return type::TypeId::INTEGER;
      case DataType::TINYINT:
        return type::TypeId::TINYINT;
      case DataType::SMALLINT:
        return type::TypeId::SMALLINT;
      case DataType::BIGINT:
        return type::TypeId::BIGINT;

      case DataType::DECIMAL:
      case DataType::DOUBLE:
      case DataType::FLOAT:
        return type::TypeId::DECIMAL;

      case DataType::BOOLEAN:
        return type::TypeId::BOOLEAN;

      // case ADDRESS:
      //  return type::Type::ADDRESS;
      //  break;

      case DataType::TIMESTAMP:
        return type::TypeId::TIMESTAMP;

      case DataType::CHAR:
      case DataType::TEXT:
      case DataType::VARCHAR:
        return type::TypeId::VARCHAR;

      case DataType::VARBINARY:
        return type::TypeId::VARBINARY;

      case DataType::INVALID:
      case DataType::PRIMARY:
      case DataType::FOREIGN:
      case DataType::MULTIUNIQUE:
      default:
        return type::TypeId::INVALID;
    }
  }

  std::unique_ptr<char[]> name = nullptr;

  // The name of the table and its database
  std::unique_ptr<TableInfo> table_info_ = nullptr;

  DataType type;
  size_t varlen = 0;
  bool not_null = false;
  bool primary = false;
  bool unique = false;
  std::unique_ptr<expression::AbstractExpression> default_value = nullptr;
  std::unique_ptr<expression::AbstractExpression> check_expression = nullptr;

  std::unique_ptr<std::vector<std::unique_ptr<char[]>>> primary_key = nullptr;
  std::unique_ptr<std::vector<std::unique_ptr<char[]>>> foreign_key_source = nullptr;
  std::unique_ptr<std::vector<std::unique_ptr<char[]>>> foreign_key_sink = nullptr;

  std::vector<char *>* multi_unique_cols = nullptr;

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

  virtual ~CreateStatement() {}

  virtual void Accept(SqlNodeVisitor* v) const override { v->Visit(this); }

  CreateType type;
  bool if_not_exists;

  std::unique_ptr<std::vector<std::unique_ptr<ColumnDefinition>>> columns;
  std::unique_ptr<std::vector<std::unique_ptr<char[]>>> index_attrs = nullptr;

  IndexType index_type;

  std::unique_ptr<char[]> index_name = nullptr;
  std::unique_ptr<char[]> trigger_name = nullptr;
  std::unique_ptr<char[]> database_name = nullptr;

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
