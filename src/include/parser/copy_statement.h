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
#include "parser/table_ref.h"
#include "expression/constant_value_expression.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace parser {

/**
 * @class CopyStatement
 * @brief Represents PSQL Copy statements.
 */
class CopyStatement : public SQLStatement {
 public:
  CopyStatement(CopyType type)
      : SQLStatement(StatementType::COPY),
        cpy_table(NULL),
        type(type),
        file_path(NULL),
        delimiter(','){};

  virtual ~CopyStatement() {
    if (file_path != nullptr) {
      delete[] file_path;
    }

    if (cpy_table != nullptr) {
      delete cpy_table;
    }
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  TableRef* cpy_table;

  CopyType type;

  char* file_path;
  char delimiter;
};

}  // End parser namespace
}  // End peloton namespace
