//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_function_statement.h
//
// Identification: src/include/parser/create_function_statement.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace parser {

struct Parameter {
  enum class FuncParamMode {
    FUNC_PARAM_IN = 'i',       /* input only */
    FUNC_PARAM_OUT = 'o',      /* output only */
    FUNC_PARAM_INOUT = 'b',    /* both */
    FUNC_PARAM_VARIADIC = 'v', /* variadic (always input) */
    FUNC_PARAM_TABLE = 't'
  };

  enum class DataType {
    INT,
    INTEGER,
    TINYINT,
    SMALLINT,
    BIGINT,
    CHAR,
    DOUBLE,
    FLOAT,
    DECIMAL,
    VARCHAR,
    TEXT,
    BOOL,
    BOOLEAN
  };

  Parameter(DataType type) : type(type){};

  virtual ~Parameter() {}

  DataType type;
  FuncParamMode mode;

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

      case DataType::CHAR:
      case DataType::TEXT:
      case DataType::VARCHAR:
        return type::TypeId::VARCHAR;

      case DataType::BOOL:
      case DataType::BOOLEAN:
        return type::TypeId::BOOLEAN;
      default:
        return type::TypeId::INVALID;
    }
  }
};

struct ReturnType : Parameter {
  ReturnType(DataType type) : Parameter(type){};
  virtual ~ReturnType() {}
};

struct FuncParameter : Parameter {
  std::string name;
  FuncParameter(std::string name, DataType type)
      : Parameter(type), name(name){};
  virtual ~FuncParameter() {}
};

class CreateFunctionStatement : public SQLStatement {
 public:
  enum class ASclause { EXECUTABLE = 0, QUERY_STRING = 1 };

  CreateFunctionStatement() : SQLStatement(StatementType::CREATE_FUNC){};

  virtual ~CreateFunctionStatement() {
    delete return_type;
    for (auto fp : *func_parameters) delete fp;
    delete func_parameters;
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  PLType language;
  ASclause as_type;
  std::vector<std::string> function_body;
  ReturnType *return_type;
  std::vector<FuncParameter *> *func_parameters;
  std::string function_name;
  bool replace = false;

  void set_as_type() {
    if (function_body.size() > 1)
      as_type = ASclause::EXECUTABLE;
    else
      as_type = ASclause::QUERY_STRING;
  }
};

}  // namespace parser
}  // namespace peloton
