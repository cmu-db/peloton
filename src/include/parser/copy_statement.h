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
#include "optimizer/query_node_visitor.h"

namespace peloton {
namespace parser {

/**
 * @struct CopyStatement
 * @brief Represents PSQL Copy statements.
 */
struct CopyStatement : SQLStatement {
  CopyStatement(CopyType type)
      : SQLStatement(STATEMENT_TYPE_COPY),
        cpy_table(NULL),
        type(type),
        file_path(NULL),
        delimiter(','){};

  virtual ~CopyStatement() {
    delete file_path;
    delete cpy_table;
  }

  TableRef* cpy_table;

  virtual void Accept(optimizer::QueryNodeVisitor* v) const override {
    v->Visit(this);
  }

  CopyType type;

  char* file_path;
  char delimiter;
};

}  // End parser namespace
}  // End peloton namespace
