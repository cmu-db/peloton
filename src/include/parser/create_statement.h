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
#include "parser/select_statement.h"
#include "common/internal_types.h"

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
    DATE,
    TIMESTAMP,
    TEXT,

    VARCHAR,
    VARBINARY
  };

  ColumnDefinition(DataType type) : type(type) {
    // Set varlen to TEXT_MAX_LENGTH if the data type is TEXT
    if (type == DataType::TEXT) varlen = type::PELOTON_TEXT_MAX_LEN;
  }

  ColumnDefinition(char *name, DataType type) : name(name), type(type) {
    // Set varlen to TEXT_MAX_LENGTH if the data type is TEXT
    if (type == DataType::TEXT) varlen = type::PELOTON_TEXT_MAX_LEN;
  }

  virtual ~ColumnDefinition() {}

  static DataType StrToDataType(char *str) {
    DataType data_type;
    // Transform column type
    if ((strcmp(str, "int") == 0) || (strcmp(str, "int4") == 0)) {
      data_type = ColumnDefinition::DataType::INT;
    } else if (strcmp(str, "varchar") == 0) {
      data_type = ColumnDefinition::DataType::VARCHAR;
    } else if (strcmp(str, "int8") == 0) {
      data_type = ColumnDefinition::DataType::BIGINT;
    } else if (strcmp(str, "int2") == 0) {
      data_type = ColumnDefinition::DataType::SMALLINT;
    } else if (strcmp(str, "timestamp") == 0) {
      data_type = ColumnDefinition::DataType::TIMESTAMP;
    } else if (strcmp(str, "bool") == 0) {
      data_type = ColumnDefinition::DataType::BOOLEAN;
    } else if (strcmp(str, "bpchar") == 0) {
      data_type = ColumnDefinition::DataType::CHAR;
    } else if ((strcmp(str, "double") == 0) || (strcmp(str, "float8") == 0)) {
      data_type = ColumnDefinition::DataType::DOUBLE;
    } else if ((strcmp(str, "real") == 0) || (strcmp(str, "float4") == 0)) {
      data_type = ColumnDefinition::DataType::FLOAT;
    } else if (strcmp(str, "numeric") == 0) {
      data_type = ColumnDefinition::DataType::DECIMAL;
    } else if (strcmp(str, "text") == 0) {
      data_type = ColumnDefinition::DataType::TEXT;
    } else if (strcmp(str, "tinyint") == 0) {
      data_type = ColumnDefinition::DataType::TINYINT;
    } else if (strcmp(str, "varbinary") == 0) {
      data_type = ColumnDefinition::DataType::VARBINARY;
    } else if (strcmp(str, "date") == 0) {
      data_type = ColumnDefinition::DataType::DATE;
    } else {
      throw NotImplementedException(
          StringUtil::Format("DataType %s not supported yet...\n", str));
    }
    return data_type;
  }

  static type::TypeId StrToValueType(char *str) {
    type::TypeId value_type;
    // Transform column type
    if ((strcmp(str, "int") == 0) || (strcmp(str, "int4") == 0)) {
      value_type = type::TypeId::INTEGER;
    } else if ((strcmp(str, "varchar") == 0) || (strcmp(str, "bpchar") == 0) ||
               (strcmp(str, "text") == 0)) {
      value_type = type::TypeId::VARCHAR;
    } else if (strcmp(str, "int8") == 0) {
      value_type = type::TypeId::BIGINT;
    } else if (strcmp(str, "int2") == 0) {
      value_type = type::TypeId::SMALLINT;
    } else if (strcmp(str, "timestamp") == 0) {
      value_type = type::TypeId::TIMESTAMP;
    } else if (strcmp(str, "bool") == 0) {
      value_type = type::TypeId::BOOLEAN;
    } else if ((strcmp(str, "double") == 0) || (strcmp(str, "float8") == 0) ||
               (strcmp(str, "real") == 0) || (strcmp(str, "float4") == 0) ||
               (strcmp(str, "numeric") == 0)) {
      value_type = type::TypeId::DECIMAL;
    } else if (strcmp(str, "tinyint") == 0) {
      value_type = type::TypeId::TINYINT;
    } else if (strcmp(str, "varbinary") == 0) {
      value_type = type::TypeId::VARBINARY;
    } else if (strcmp(str, "date") == 0) {
      value_type = type::TypeId::DATE;
    } else {
      throw NotImplementedException(
          StringUtil::Format("DataType %s not supported yet...\n", str));
    }
    return value_type;
  }

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

      case DataType::TIMESTAMP:
        return type::TypeId::TIMESTAMP;

      case DataType::CHAR:
      case DataType::TEXT:
      case DataType::VARCHAR:
        return type::TypeId::VARCHAR;

      case DataType::VARBINARY:
        return type::TypeId::VARBINARY;

      case DataType::DATE:
        return type::TypeId::DATE;

      case DataType::INVALID:
      case DataType::PRIMARY:
      case DataType::FOREIGN:
      case DataType::MULTIUNIQUE:
      default:
        return type::TypeId::INVALID;
    }
  }

  std::string name;

  // The name of the table and its database
  std::unique_ptr<TableInfo> table_info_ = nullptr;

  DataType type;
  size_t varlen = 0;
  bool not_null = false;
  bool primary = false;
  bool unique = false;
  std::unique_ptr<expression::AbstractExpression> default_value = nullptr;
  std::unique_ptr<expression::AbstractExpression> check_expression = nullptr;

  std::vector<std::string> primary_key;
  std::vector<std::string> foreign_key_source;
  std::vector<std::string> foreign_key_sink;

  std::vector<std::string> multi_unique_cols;

  std::string foreign_key_table_name;
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
  enum CreateType { kTable, kDatabase, kIndex, kTrigger, kSchema, kView };

  CreateStatement(CreateType type)
      : TableRefStatement(StatementType::CREATE),
        type(type),
        if_not_exists(false){};

  virtual ~CreateStatement() {}

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  CreateType type;
  bool if_not_exists;

  std::vector<std::unique_ptr<ColumnDefinition>> columns;

  std::vector<std::string> index_attrs;
  IndexType index_type;
  std::string index_name;

  std::string schema_name;

  std::string view_name;
  std::unique_ptr<SelectStatement> view_query;

  bool unique = false;

  std::string trigger_name;
  std::vector<std::string> trigger_funcname;
  std::vector<std::string> trigger_args;
  std::vector<std::string> trigger_columns;
  std::unique_ptr<expression::AbstractExpression> trigger_when;
  int16_t trigger_type;  // information about row, timing, events, access by
                         // pg_trigger
};

}  // namespace parser
}  // namespace peloton
